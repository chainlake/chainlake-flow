
### unified DLQ topic
`evm.bsc.mainnet.dlq`

### 🔁 Retry pipeline
`DLQ Topic → Retry Service → Main Topic`
- auto retry (N times)
- exponential backoff
- manual replay UI

### 🧠 DLQ tiers
DLQ replay servcie (retry, backoff, idempotency)

- DLQ_L1 → temporary errors (retryable)
- DLQ_L2 → permanent errors (manual fix)


```text
                ┌──────────────┐
                │ BlockSource  │
                └──────┬───────┘
                       ↓
                ┌──────────────┐
                │   Engine     │
                └──────┬───────┘
                       ↓
              ┌───────────────┐
              │ Kafka Topics  │
              └──────┬────────┘
                     ↓
               ┌──────────┐
               │   DLQ    │  ← unified topic
               └────┬─────┘
                    ↓
        ┌────────────────────────┐
        │ Retry / Debug / Replay │
        └────────────────────────┘
```


```text
                ┌──────────────────────┐
                │   Ingestion Engine   │
                └─────────┬────────────┘
                          ↓
               (failure happens anywhere)
                          ↓
        ┌──────────────────────────────────┐
        │        DLQ Router Layer          │
        └────────────────┬─────────────────┘
                         ↓
        ┌──────────────┬──────────────┬──────────────┐
        │ Kafka DLQ    │ Retry Queue  │ Replay Store │
        └──────────────┴──────────────┴──────────────┘
```

```
| Option                         | Use case           | Verdict          |
| ------------------------------ | ------------------ | ---------------- |
| Kafka DLQ topic                | streaming systems  | ✅ BEST PRACTICE  |
| Redis queue                    | short-lived retry  | ⚠️ optional      |
| In-memory queue                | debugging only     | ❌ not production |
| Database (ClickHouse/Postgres) | analytics / replay | ✅ complement     |
| S3 / object storage            | long-term archive  | ✅ optional       |
```

# unify DLQ + retry + replay system
```json
{
  "id": "uuid-or-hash",
  "chain": "evm",
  "network": "bsc-mainnet",

  "pipeline": "block", 
  "entity": "transaction",

  "block_number": 90000100,

  "stage": "rpc | processor | sink | downstream",
  "error_type": "TimeoutError | DecodeError | ValidationError",
  "error_message": "...",

  "payload": {...},         // optional raw data
  "context": {...},         // rpc params, request info

  "retry_count": 0,
  "max_retry": 3,

  "status": "pending | retrying | failed | resolved",

  "first_seen_at": "...",
  "last_attempt_at": "...",
  "next_retry_at": "...",

  "ingest_timestamp": "..."
}
```

`dlq.ingestion`
key = f"{chain}:{entity}:{block_number}"


## Retry architecture
```
Kafka DLQ Topic
      ↓
Retry Worker (async service)
      ↓
Re-inject into Engine OR RPC layer
```

## Retry flow
1. DLQ receives failed message
2. Retry worker consumes
3. Checks:

## Retry backoff strategy

## Replay system (manual / batch)
Replay use case
- RPC bug fixed
- processor bug fixed
- historical reprocessing

### Replay architecture
```text
DLQ topic / storage
      ↓
Replay CLI / API
      ↓
Re-create BlockSource
      ↓
engine.run_stream(...)
```
example: 
```python
replay_source = DLQReplayBlockSource(
    filter={"entity": "transaction", "status": "failed"}
)

await engine.run_stream(replay_source)
```

## Final architecture summary
```text
                ┌────────────────────┐
                │  Ingestion Engine  │
                └─────────┬──────────┘
                          ↓
                (error occurs)
                          ↓
                ┌────────────────┐
                │   DLQ Sender   │
                └────────┬───────┘
                         ↓
               Kafka Topic: dlq.ingestion
                         ↓
        ┌───────────────┴───────────────┐
        ↓                               ↓
 Retry Worker                    Replay System
        ↓                               ↓
  Re-ingest                     Backfill Source
```

# 3️⃣ Unified schema validator (prevent bad DLQ writes)

fetcher → processor → ✅ validator → engine → sink
engine error → ✅ validator → DLQ sink