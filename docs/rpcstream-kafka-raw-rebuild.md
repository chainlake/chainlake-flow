# rpcstream 数据拉取收敛 + Kafka 原始层重设计方案

> 状态：草案，待评审
> 范围：消除多 shard 对同一区块的重复上游 RPC；为 `block/transaction/log/token_transfer`
> 建立“一次规范拉取 → Kafka 原始数据 → 各下游消费派生”的形态
> 关联：`apps/rpcstream` 与 `apps/rpcstream-backfill`（chainlake-infra）

---

## 1. 背景与问题

### 1.1 现状：每个 shard 各自向上游打同样的两块数据

rpcstream 按“每个 entity 一个独立进程 shard + 独立 Kafka producer + 独立 watermark
identity”拆开（详见 `rpcstream-config-core/log/token-transfer.yaml` 注释：最初为了绕开
单进程串行 producer 的行数上限）。但代价是 **RPC 也被拆开重复**：

`rpcstream/adapters/evm/fetcher.py` 中每个进程对每个 block 的请求只取决于其内部 entity 集合
（`dag.ENTITY_DEPENDENCIES` 解析）：

| shard | 内部 entity | 每块上游请求 |
|---|---|---|
| core (block+transaction) | `{block, transaction, receipt}` | `eth_getBlockByNumber(n, true)` + `eth_getBlockReceipts(n)` |
| log (log) | `{block, receipt, log}` | `eth_getBlockByNumber(n, false)` + `eth_getBlockReceipts(n)` |
| token_transfer (已暂停) | `{block, receipt, log}` | 与 log 完全相同 |

即 **同一个 block**，上游会收到：

- `eth_getBlockByNumber`：每个 shard 各 1 次（core 带完整 tx，log/token 只取头）；
- `eth_getBlockReceipts`：每个 shard 各 1 次，方法/参数完全相同（receipts 响应还很大）。

在免费上游晚间限流/长尾（见 `docs` 相关诊断：p99 从 ~400ms 飙到 ~4s、`rate_limited`
持续）时，这些重复调用只是白白消耗配额，并不会带来更多有效数据。

### 1.2 数据完备性：两个 RPC 足够派生所有目标实体

- `eth_getBlockByNumber(n, true)` → **block header** + **transaction**（含完整 tx 字段）；
- `eth_getBlockReceipts(n)` → **receipt**（status/gas/…）+ **log**（receipt.logs）；
- **token_transfer** = 对 log（ERC20/ERC721/ERC1155 的 topics+data）解码派生。

> 例外：**trace 不在本最小集内**，`debug_traceBlock` 需要另行拉取；本方案只约束上面 5 类。

### 1.3 结论

应当把“拉取”和“派生”解耦：**全链只有一个（或极少量）拉取者**，按块把 `block + receipts`
写入 Kafka 原始层；`core/log/token_transfer` 全部改为消费该原始层后本地解析/派生，
从而对上游每个块只发 2 个请求（当前 3 shard 是 6 个，token 暂停后仍 4 个）。

---

## 2. 目标架构

```
           ┌──────────────── 上游 RPC (erpc) ────────────────┐
           │       每块只发 2 个请求                          │
           │   eth_getBlockByNumber(n, true)                 │
           │   eth_getBlockReceipts(n)                       │
           └───────────────┬──────────────────────────────┘
                           ▼
              ┌──────────────────────────┐
              │  canonical-fetch（拉取者） │  ← 每块一个 watermark 游标，
              │  校验、按块写 Kafka       │     可 realtime 也可 backfill(range)
              └────────────┬─────────────┘
                           ▼
              ┌──────────── Kafka 原始层 ────────────┐
              │  （如下 §3 的消息粒度两种方案）         │
              └───────┬──────────────┬──────────────┘
                      ▼              ▼
             ┌────────────────┐ ┌─────────────────────┐
             │ 派生 consumer   │ │ 后续可选 Flink/SQL  │
             │ core/log/      │ │ 或新增下游           │
             │ token_transfer │ └─────────────────────┘
             │ parse + sink   │
             └────────────────┘
```

关键点：

1. **拉取者与派生者彻底分离**：派生进程不持有上游 RPC 配置/配额；加新下游（如 token
   从 `raw_log` 重建）只是加一个 consumer。
2. **派生天然可水平扩展**：同一原始层可被多个副本消费（按 block partition/key），不受
   “单进程解析行数 + 单 producer 串行”限制——这正是历史上被迫把实体拆 shard 的根因，
   现在把“扇出点”从 RPC 层搬到 Kafka 层。
3. 断点续跑语义沿用现有 checkpoint：拉取者与派生者各自维护“已连续消费/提交的 block
   水位”，backfill 就是给定 `[from, to]` 区间跑同一 pipeline。

---

## 3. 消息粒度（关键决策）

| 方案 | 拉取者产出 | 派生方式 | 优点 | 风险 |
|---|---|---|---|---|
| **A. 每块信封**（推荐先验证） | 每块 1–2 条消息：`{ block_full_json }` 与 `{ receipts_json }`（或合成一条） | consumer 自己跑现有 parser/decoder 后各落各的 sink topic | 消息量小；彻底避开“单 producer 行数上限”；raw 留存占盘小 | receipts/block JSON 单条体积大（需定上限/分块）；解析在消费侧重复 CPU |
| **B. 规范化行 topic** | 拉取者在写入前先 parse，落 `bsc.raw_block / raw_transaction / raw_receipt / raw_log` 等行 | consumer 直接消费现成行做最薄写入/二次派生 | 消费侧最薄、schema 统一（复用现有 avro/protobuf） | 拉取者要一个进程顶所有 entity 的行数峰值 → 大概率复现“producer 串行瓶颈” |

**建议**：优先按 **A（信封）** 落地拉取层（消息量小、瓶颈小），派生 consumer 内复用
现有 `parse_*`/decoder/实体处理逻辑；当吞吐与 schema 需求成熟后，再在派生侧把高频实体
物化为 **B** 的行 topic 供更薄消费（token 从 `raw_log` 重建即属此类）。

> 落地前用 erpc 已有的 `erpc_upstream_response_size_bytes` 指标实测每个 block 的
> block/receipts 响应大小与 p95，用于定 Kafka 单条上限与分区/retention（见 §6）。

### 3.1 主题与 schema 约定（沿用现有命名）

现有命名模板 `topic_template = "{namespace}.{kind}_{entity}"`（`config/schema.py`），
`topic_kind_for_entity`（`dag.py`）给出 `transaction → enriched`、其余 → `raw`。
派生写出的 sink topic 建议沿用：`bsc.raw_block / bsc.enriched_transaction /
bsc.raw_receipt / bsc.raw_log`；`token_transfer` 重建后可写
`bsc.raw_token_transfer`（或沿用现有命名）。

派生用同一套 Schema Registry（现有 `kafka.schemaRegistry` avro/protobuf 配置）即可，
消费者需要注册消费的 schema，与现有 avro 消费路径一致。

---

## 4. 水位、一致性、错误处理

1. **拉取者水位**：沿用 `WatermarkManager`/`cursor_state`（`pipeline|entities` 键）按块
   提交连续水位；backfill 与 realtime 同构（现有 `bsc_mainnet_backfill_*` 身份已验证）。
2. **派生者水位**：每个派生 consumer 独立维护“已消费并成功落 sink 的最大连续块”水位；
   同一 pipeline identity 规则，断点续跑从 last commit 继续。
3. **幂等/EOS**：写 Kafka 沿用现有 `kafka.eos` 选项（当前默认关）。派生重跑允许按 block
   key 幂等覆盖；若开启 EOS 需保证 producer 事务与水位提交一致。
4. **失败**：拉取失败（上游错误）沿用现有 cursor failed + gap + DLQ 语义；派生解析失败
   落到 `dlq.ingestion`，由现有 `rpcstream-dlq-retry-*` 或新的 retry consumer 处理。
5. **乱序/迟到**：信封按 block 有序入 topic；派生内“receipts/log 派生 token”等块内顺序
   与现有 `INTERNAL_ENTITY_ORDER` 一致处理；跨 block 仅做连续水位收敛，不跨 block join。

---

## 5. 迁移路径

- **M0 度量**（先做）：用 erpc `response_size_bytes`/`attempt_outcome` 观测每块
  block/receipts 体积与限流率，确定 Kafka 单条/分区/retention 参数与预期 RPC 节省。
- **M1 POC**：新增 `canonical-fetch` 形态（或扩展现有 engine 的拉取模式），按块信封写
  原始层；单个派生 consumer（先做 `log` 或 `token`）从 Kafka 重建并落原 sink topic，
  与现网并行跑一段，对账 block/log/token 行数与水位。
- **M2 迁移**：core/log 切到派生消费；token_transfer 依赖 raw_log 的新 pipeline 恢复
  （先解除 `apps/rpcstream` 中 `rpcstream-token-transfer`/`dlq-retry-token-transfer`
  的 replicas=0 暂停）。
- **M3（可选）**：把派生侧迁移到 Flink，从 Kafka 信封/行 topic 做 SQL/状态计算；
  决策点在于“是否值得用 Java 重写 EVM parser”，短期建议继续复用 Python。

> 过渡期保留现有 RPC 形态作为回退；canonical 与旧形态共享同一 erpc 配额期间，注意错峰。

---

## 6. 数据与成本（待 M0 实测）

- RPC 节省：目标由“N 个 shard × 2 请求/块”收敛为“2 请求/块”（全链一份）。
- 原始层消息体积：`block_full + receipts` 明显大于任意单一实体行集之和；
  用 erpc 的响应体积指标实测后决定：
  - 是否需要把 receipts 按 block 分成多条（如按 500 条/批）避免单条超限；
  - raw 层 retention（建议先用 1–3 天 + 派生完成后可压缩/清理），
  - 分区数按 block 取模，使派生并发消费能按块并行。
- CPU/网络：拉取侧解码开销前移、派生侧解析开销后移并水平扩展；净效果取决于消息粒度。

---

## 7. 未决决策（评审时讨论）

1. **信封 vs 行**：默认 A 信封起步，但需要拍板 raw 层是否也保留“规范化行”双写。
2. **拉取者数量**：全链 1 个（含 realtime + backfill 共用同一模式），还是按 range 分区
   多实例（每实例负责一段并各自写信封）——两者都只对每块发 2 个请求。
3. **信封内容**：`blockFull + receipts` 一起 vs 分开两条（分开利于只消费 receipts 的下游）。
4. **谁解析、谁落行**：token 从 `raw_log` 重建时，`raw_log` 是“信封内解析出的行 topic”
   还是“信封的直接消费者二次解析”。
5. **schema 演进**：raw 信封用 JSON/avro byte 包裹（演进成本最低）还是严格 schema。
6. **与 erpc 层短期优化的取舍**：若 erpc 支持按 `method+params` 的结果缓存，可先通过缓存
   消除同一 receipts 的重复请求作为 M0 前的最小改动；本方案是缓存不可行/不够时的结构化解法。

---

## 附录：现有代码事实（供实现引用）

- `rpcstream/adapters/evm/fetcher.py`：按 entity 集合组请求（现状重复的出处）。
- `rpcstream/adapters/evm/dag.py`：`ENTITY_DEPENDENCIES` / `topic_kind_for_entity`。
- `rpcstream/adapters/evm/rpc_requests.py`：`build_get_block_by_number`（`include_transactions`）、
  `build_get_block_receipts`。
- `rpcstream/state/checkpoint.py`：`WatermarkManager` + `cursor_state`/commit 水位与
  `set_backfill_range`/Argo progress（`ARGO_PROGRESS_FILE`）。
- `rpcstream/config/schema.py`：`topic_template="{namespace}.{kind}_{entity}"`、kafka eos/schema 配置。
- `apps/rpcstream/*`（chainlake-infra）：core/log/token shard 的 Deployment/ConfigMap；
  token 已暂停（replicas=0），待本方案恢复。
