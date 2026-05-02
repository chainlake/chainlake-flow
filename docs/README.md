# Docs Index

This directory contains the main design and operational notes for Chainlake Flow.

The current runnable system is centered on:

- `rpcstream` as the unified CLI
- EVM ingestion over RPC
- Kafka sink with optional EOS
- `commit_watermark` and `cursor_state` based recovery
- DLQ retry and replay
- benchmark dashboard output

## Start Here

If you are new to the project, read these first:

1. [../README.md](../README.md)
2. [ingestion_flow.md](ingestion_flow.md)
3. [kafka_eos.md](kafka_eos.md)
4. [rpcstream_progress_dashboard.png](rpcstream_progress_dashboard.png)

## CLI

Main runtime entrypoints:

```bash
rpcstream
rpcstream --from chainhead
rpcstream --from 95000000
rpcstream --from 95000000 --to 95000100
rpcstream init
rpcstream benchmark --mode backfill --sink blackhole --output-file benchmark.json
rpcstream benchmark --mode realtime --sink kafka
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
- `rpcstream benchmark`
  benchmark runtime with a live progress dashboard, cursor timing details, and
  recent info-level logs

## Core Runtime Docs

- [ingestion_flow.md](ingestion_flow.md)
  End-to-end ingestion path and runtime stages
- [async_rpc_scheduler.md](async_rpc_scheduler.md)
  Adaptive RPC concurrency and scheduler design
- [pipeline.md](pipeline.md)
  Pipeline config and EVM entity model

## Kafka and Recovery

- [kafka_eos.md](kafka_eos.md)
  `commit_watermark`, `cursor_state`, EOS, and recovery semantics
- [dlq.md](dlq.md)
  DLQ schema, retry, replay, and operational flow

## Operations and Debugging

- [debug_and_trace_block.md](debug_and_trace_block.md)
  Debugging a specific block and tracing ingestion behavior
- [observability.md](observability.md)
  Tracing, metrics, and logs
- [rpcstream_progress_dashboard.png](rpcstream_progress_dashboard.png)
  Benchmark progress dashboard screenshot
- [e2e_lantency.md](e2e_lantency.md)
  End-to-end latency notes

## Environment and Deployment

- [docker.md](docker.md)
  Container-oriented usage notes
