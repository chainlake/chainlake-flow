
# Observability

```text
[ Ingestion Pipeline ]
    ↓
(OTEL SDK: logs + metrics + traces)
    ↓
[ OTEL Collector ]
    ↓
 ┌───────────────┬───────────────┬───────────────┐
 │   Prometheus  │     Tempo     │     Loki      │
 │   (metrics)   │   (traces)    │    (logs)     │
 └───────────────┴───────────────┴───────────────┘
                ↓
             Grafana
```

## Network boundary observability

| Event             | Level   | Why                    |
| ----------------- | ------- | ---------------------- |
| RPC request start | `debug` | trace-level visibility |
| RPC success       | `info`  | main happy path        |
| Retry             | `warn`  | important signal       |
| Timeout           | `warn`  | degradation            |
| Transport error   | `error` | failure                |
| Final failure     | `error` | critical               |
