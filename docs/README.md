# Docs Index

This directory contains the main design and operational notes for Chainlake Flow.

The current runnable system is centered on:

- `rpcstream` as the unified CLI
- EVM ingestion over RPC
- Kafka sink with optional EOS
- `commit_watermark` and `cursor_state` based recovery
- DLQ retry and replay

## Start Here

If you are new to the project, read these first:

1. [../README.md](chainlake-flow/README.md)
2. [ingestion_flow.md](chainlake-flow/docs/ingestion_flow.md)
3. [kafka_eos.md](chainlake-flow/docs/kafka_eos.md)

## CLI

Main runtime entrypoints:

```bash
rpcstream
rpcstream --from chainhead
rpcstream --from 95000000
rpcstream --from 95000000 --to 95000100
rpcstream init
rpcstream dlq retry
rpcstream dlq replay --entity trace --status pending --stage processor --max-records 5
rpcstream config validate
rpcstream config print
```

Semantics:

- `rpcstream`
  realtime from `checkpoint`; if no checkpoint exists yet, start from chainhead
- `rpcstream --from chainhead`
  ignore saved progress and start from the current chainhead
- `rpcstream --from <cursor>`
  realtime from a specific cursor
- `rpcstream --from <cursor> --to <cursor>`
  bounded backfill
- `rpcstream init`
  optional environment preparation only; it creates topics and pre-registers
  protobuf schemas, but it is not required before starting ingestion because
  schemas are auto-registered on first write

## Core Runtime Docs

- [ingestion_flow.md](chainlake-flow/docs/ingestion_flow.md)
  End-to-end ingestion path and runtime stages
- [block_source.md](chainlake-flow/docs/block_source.md)
  Realtime and backfill cursor/block source behavior
- [async_rpc_scheduler.md](chainlake-flow/docs/async_rpc_scheduler.md)
  Adaptive RPC concurrency and scheduler design
- [pipeline_and_entites.md](chainlake-flow/docs/pipeline_and_entites.md)
  Pipeline config and EVM entity model

## Kafka and Recovery

- [kafka_eos.md](chainlake-flow/docs/kafka_eos.md)
  `commit_watermark`, `cursor_state`, EOS, and recovery semantics
- [dlq.md](chainlake-flow/docs/dlq.md)
  DLQ schema, retry, replay, and operational flow

## Operations and Debugging

- [debug_and_trace_block.md](chainlake-flow/docs/debug_and_trace_block.md)
  Debugging a specific block and tracing ingestion behavior
- [observability.md](chainlake-flow/docs/observability.md)
  Tracing, metrics, and logs
- [progress_dashbaord.md](chainlake-flow/docs/progress_dashbaord.md)
  Progress and operational dashboard ideas
- [e2e_lantency.md](chainlake-flow/docs/e2e_lantency.md)
  End-to-end latency notes

## Environment and Deployment

- [docker.md](chainlake-flow/docs/docker.md)
  Container-oriented usage notes

## Chain-Specific Notes

- [evm_method.md](chainlake-flow/docs/evm_method.md)
  EVM RPC methods and related notes
