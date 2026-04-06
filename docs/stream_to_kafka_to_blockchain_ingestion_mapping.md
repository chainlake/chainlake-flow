# stream_to_kafka 到 blockchain_ingestion 功能映射分析

## 1) 结论摘要

- `stream_to_kafka` 目录是当前**可运行实现**，包含完整的实时/回填 ingestion 逻辑（planner、RPC、排序、状态、Kafka EOS、metrics、追头等）。
- `blockchain_ingestion` 目录目前是按 README 规划好的**目标模块骨架**，目录划分与职责已明确，但文件内容当前为空，尚未迁移实现。
- 两者不是“平级重复实现”，而更像是：`stream_to_kafka`（现有实现）→ `blockchain_ingestion`（重构后的标准包结构）。

## 2) 依据：README 对目标架构的定义

README 明确了目标引擎应包含：

- 统一实时 + 回填引擎
- 异步 RPC 并发
- 有序输出保障
- Kafka 异步投递
- 可扩展多链 adapter

并给出了 `blockchain_ingestion` 分层：

- `adapters/`
- `rpc/`
- `planner/`
- `runtime/`
- `execution/`
- `state/`
- `sinks/`
- `metrics/`
- `models/`
- `utils/`
- `config/`

## 3) stream_to_kafka 现有能力分解

### A. 运行主循环 / 编排层

`stream_to_kafka/src/ingestion/ingestion_engine.py` 的 `IngestionEngine` 实现了统一循环：

- 启动 `LatestBlockTracker`
- 预填 inflight ranges
- 异步等待任务完成
- 错误重试（`RangeRegistry.mark_retry`）
- 放入 `OrderedResultBuffer`
- 顺序提交并优雅关闭

对应目标模块：

- `blockchain_ingestion/runtime/engine.py`
- `blockchain_ingestion/runtime/scheduler.py`
- `blockchain_ingestion/runtime/dispatcher.py`
- `blockchain_ingestion/runtime/lifecycle.py`

### B. 规划层（实时/回填）

`stream_to_kafka/src/planning/range_planner.py` 已有：

- `BaseRangePlanner`
- `TailingRangePlanner`（追头）
- `BoundedRangePlanner`（有界回填）

对应目标模块：

- `blockchain_ingestion/planner/range_planner.py`
- `blockchain_ingestion/planner/block_window.py`
- `blockchain_ingestion/planner/stream_cursor.py`

### C. 执行与有序缓冲

`stream_to_kafka/src/execution/ordered_buffer.py` 提供按 `range_id` 顺序释放结果。`stream_to_kafka/src/execution/range_result.py` 定义执行结果载体。

对应目标模块：

- `blockchain_ingestion/execution/ordered_buffer.py`
- `blockchain_ingestion/execution/result_merger.py`
- `blockchain_ingestion/execution/fetcher.py`
- `blockchain_ingestion/execution/parser_executor.py`

### D. 状态与控制面

`stream_to_kafka/src/control/range_registry.py` + `range_record.py` + `range_status.py` 管理 range 生命周期（PLANNED/INFLIGHT/RETRYING/DONE/FAILED）和重试。

对应目标模块：

- `blockchain_ingestion/state/range_registry.py`
- `blockchain_ingestion/state/checkpoint.py`
- `blockchain_ingestion/state/cursor_store.py`
- `blockchain_ingestion/state/replay_state.py`

### E. RPC 传输与容错

`stream_to_kafka/src/rpc_provider.py` 包含：

- `AsyncRpcClient`（aiohttp JSON-RPC）
- `RpcProvider`（按权重+退避）
- `RpcPool / GroupedRpcPool`
- `Web3AsyncRouter`
- `AsyncRpcScheduler`
- 限速 key slot、失败退避、429/错误分类

对应目标模块：

- `blockchain_ingestion/rpc/async_client.py`
- `blockchain_ingestion/rpc/erpc_client.py`
- `blockchain_ingestion/rpc/retry.py`
- `blockchain_ingestion/rpc/rate_limit.py`
- `blockchain_ingestion/rpc/timeout.py`

### F. Kafka 产出与 EOS

`stream_to_kafka/src/kafka_utils.py` / `kafka_utils_EOS.py` 包含 producer 初始化、序列化器加载、事务参数；`stream_ingest_logs.py` 中实现事务 begin/produce/commit。

对应目标模块：

- `blockchain_ingestion/sinks/kafka/producer.py`
- `blockchain_ingestion/sinks/kafka/serializer.py`
- `blockchain_ingestion/sinks/kafka/eos.py`
- `blockchain_ingestion/sinks/base.py`

### G. 指标与可观测性

`stream_to_kafka/src/metrics/definitions.py` + `metrics/runtime.py` + `metrics/context.py` 提供 Prometheus 指标和 ContextVar 绑定。

对应目标模块：

- `blockchain_ingestion/metrics/definitions.py`
- `blockchain_ingestion/metrics/runtime.py`
- `blockchain_ingestion/metrics/exporter.py`
- `blockchain_ingestion/metrics/tracing.py`

### H. 追踪链头

`stream_to_kafka/src/tracking/latest_block_tracker.py` 提供链头缓存、刷新、reorg 容忍、异常跳变保护。

对应目标模块：

- `blockchain_ingestion/planner/stream_cursor.py`
- （部分逻辑也可放 `runtime/` 作为链头服务）

### I. 入口作业

`stream_to_kafka/web3_realtime_logs.py`、`web3_realtime_blocks.py`、`backfill_jobs/*` 当前承担 CLI/job 入口职责。

对应目标模块：

- `cli/realtime.py`
- `cli/backfill.py`
- `cli/logs.py`

## 4) 一对一映射表（建议落位）

| stream_to_kafka | blockchain_ingestion 对应位置 | 说明 |
|---|---|---|
| `src/ingestion/ingestion_engine.py` | `runtime/engine.py` | 主循环编排 |
| `src/ingestion/stream_ingest_logs.py` | `runtime/engine.py` + `execution/*` + `sinks/kafka/*` | 当前是“编排+执行+提交”混合实现，建议拆层 |
| `src/planning/range_planner.py` | `planner/range_planner.py` | 实时/回填 planner |
| `src/planning/block_range.py` | `planner/block_window.py` 或 `models/block.py` | range 数据结构 |
| `src/control/range_registry.py` | `state/range_registry.py` | 生命周期状态机 |
| `src/control/range_record.py` | `state/range_registry.py`（内部 dataclass）或 `models` | 控制面记录 |
| `src/control/range_status.py` | `state/range_registry.py` | 状态枚举 |
| `src/execution/ordered_buffer.py` | `execution/ordered_buffer.py` | 顺序释放 |
| `src/execution/range_result.py` | `execution/result_merger.py` 或 `models/log.py`/`transaction.py` | 执行结果封装 |
| `src/rpc_provider.py` | `rpc/async_client.py` + `rpc/retry.py` + `rpc/rate_limit.py` + `rpc/timeout.py` + `rpc/erpc_client.py` | RPC 核心能力 |
| `src/kafka_utils.py` | `sinks/kafka/producer.py` + `sinks/kafka/serializer.py` | Kafka 初始化和序列化 |
| `src/kafka_utils_EOS.py` | `sinks/kafka/eos.py` | EOS/事务语义 |
| `src/metrics/*` | `metrics/*` | 指标与上下文 |
| `src/tracking/latest_block_tracker.py` | `planner/stream_cursor.py` | 链头追踪/游标 |
| `src/state.py` | `state/checkpoint.py` + `state/cursor_store.py` | Kafka 状态恢复 |
| `web3_realtime_logs.py` | `cli/logs.py` / `cli/realtime.py` | 日志流入口 |
| `web3_realtime_blocks.py` | `cli/realtime.py` | 区块流入口 |
| `backfill_jobs/*` | `cli/backfill.py` | 历史回填入口 |

## 5) 当前差距（非常关键）

- `blockchain_ingestion` 文件结构已建好，但尚无实现内容。
- 因此“对应功能”目前是**架构映射关系**，而不是“已完成代码迁移关系”。
- 若要真正完成迁移，建议按以下优先级：
  1. 先迁 `planner` + `state` + `execution/ordered_buffer`
  2. 再迁 `rpc`
  3. 最后拆分 `stream_ingest_logs.py` 的 Kafka 提交逻辑进入 `sinks/kafka/*`

