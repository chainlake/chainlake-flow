# rpcstream 数据拉取收敛 + Kafka 原始层重设计方案

> 状态：草案，待评审
> 范围：消除多 shard 对同一区块的重复上游 RPC；为 `block/transaction/log/token_transfer`
> 建立“一次规范拉取 → Kafka 原始数据 → 各下游消费派生”的形态
> 决策 v2：推荐 **Kafka(raw) + Flink(derived)** 派生链路（见 §8），**不引入 Fluss**
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

## 8. 推荐实现链路：Kafka(raw) → Flink → derived → ClickHouse（不引入 Fluss）

在 §2 的分层基础上，派生层采用 **Flink**，raw 层继续用 Kafka（现有 redpanda），
不引入 Fluss：

```
rpc fetcher (python, canonical, 每块 2 请求)
   │  写信封或规范化行到 raw topic
   ▼
Kafka raw (redpanda, 现有)         ← 保留现有 watermark/DLQ/producer 语义
   │
   ▼
Flink (SQL 或 PyFlink UDF；Java 重写为可选项)
   │  解析/轻派生 + 乱序收敛（§8.2）
   ▼
derived Kafka topic（如 bsc.enriched_transaction / bsc.raw_token_transfer / …）
   │
   ├─(推荐) ClickHouse Kafka Engine 表直接消费，或
   └─ Flink sink 写 ClickHouse
```

### 8.1 为什么不用 Fluss（本阶段）

- 集群已有 redpanda(Kafka)、Flink、ClickHouse，rpcstream 的 producer/watermark/DLQ 语义
  都建立在 Kafka 上；Fluss 是新常驻有状态系统，且本轮目标只是“消除重复 RPC +
  一次拉取多派生”，Kafka 已足够。
- 单节点 `cln001` 资源已接近满载；Fluss server + 配套 Flink job 会显著增加常住资源。
- Fluss 生态仍在快速演进，Python 侧写入（官方 client / Kafka 兼容层）成熟度需 POC 验证。
- Fluss 的价值主要在“同一份数据同时给流派生 + OLAP/湖直接读”的实时湖仓场景；若未来
  确有此需求，可作为独立 POC（小流量镜像 raw 层验证），不与本次 RPC 收敛绑定。

### 8.2 raw → derived 的解析归属（工作量决策，按推荐序）

| 方案 | raw 内容 | Flink 里做什么 | 评价 |
|---|---|---|---|
| **A（推荐起步）** | 规范化行（`raw_block/raw_receipt/raw_log`…） | 只做**轻量 SQL**：filter/project/join/聚合/事件时间 | 简单可靠；但要避免回退到“单 producer 行级扇出”瓶颈（可由多 partition + 多 writer 规避，见 §3 决策） |
| **B1** | 信封（blockFull+receipts JSON） | **PyFlink UDF** 复用现有 `parse_*`/decoder | 复用 Python parser、改动小；需把 python 解析依赖打包进 Flink job，吞吐受 UDF 性能限制 |
| **B2（长期可选）** | 信封 | **Flink Java 重写** EVM 解析 | 最正统、性能最好，但需把现有 EVM decoder（topics/data、ERC20/721/1155、tx+receipt 合并）全部翻写为 Java，工作量最大 |

> 建议：A 或 B1 先跑通并量化吞吐；B2 只有在确认长期 Flink 化且吞吐要求时才投入。

### 8.3 乱序到达处理（四层，自下而上可选叠加）

BSC 按 block 单调，同一块信封内天然有序；乱序只会出现在**块级**（重试重放、backfill 与
realtime 同 key 并存、多分区、producer 重发）。按成本从低到高：

1. **L0 源头保序**：canonical fetch 以 block 号为 partition key 顺序写同一 partition，
   同一 block 永远只写一次（生产者幂等/去重）；raw 层几乎不乱序。
2. **L1 事件时间容忍**：Flink 以 block 时间戳为事件时间 + watermark，允许小窗口内的
   迟到/乱序（`allowedLateness` 一小段），主流数据在此即收敛。
3. **L2 连续水位发射（推荐，决定正确性）**：Flink 内用 block 号 keyed state（RocksDB）
   + 定时器实现 **gap-buffer**：收到的块先入缓存，只有连续无缺口的一段才发射到 derived；
   重复块按号幂等丢弃。本质是复用 rpcstream `WatermarkManager` 的“连续水位 + gap”语义。
   该层让 backfill/重放天然安全。
4. **L3 迟到超窗兜底**：超出 watermark 允许范围的迟到数据进入侧输出流（对应现有
   `dlq.ingestion` 思路），由低优先级 job/回填任务补齐到 derived，保证最终一致且不阻塞主链路。

ClickHouse 侧（如用 Kafka Engine 表）是追加语义，建议：
- Flink 保证“每 block 只发一次、块级有序”；
- CH 表用 `ReplacingMergeTree`/以 `block_number`（或 `chain+entity+block`）为幂等键兜底
  重复/迟到覆盖。

### 8.4 与现有 Flink 资产整合

Argo 已有 `flink-operator`/`flink-session-cluster` 以及
`flink-bsc-realtime-enrichment/metrics` 两个作业。落地前先确认：
- 现有 enrichment/metrics 作业是否已经消费 `bsc.raw_*` 并写 ClickHouse/derived；
- 若是，新的 derived（log/token_transfer 等）优先**复用同一 Flink session/作业框架**，
  只加 SQL 作业，避免再起一套集群。

### 8.5 与本方案其余章节的关系

- raw 消息粒度（信封 vs 行）见 §3；L2 gap-buffer 与该粒度无关，可叠加。
- 水位/一致性/DLQ 通用语义见 §4，L2 只是把这些语义搬到 Flink 侧。
- 迁移阶段仍遵循 §5 的 M0–M3；M2 的目标形态按本节的“Kafka(raw)→Flink→derived→CH”落地，
  而不是 §2 的“Python 派生 consumer”形态（后者作为 Flink 尚未就绪时的过渡可保留）。
- **erpc 缓存（§7 决策 6）仍是先做的最小改动**，与本节链路不冲突、可并行推进。

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
