# DLQ

`chainlake-flow` uses one unified DLQ topic:

- topic: `dlq.ingestion`

This topic stores the latest state of failed ingestion records and is used by:

- normal pipeline DLQ writes
- automatic `retry`
- manual `replay`

## Status

Each DLQ record has one of these statuses:

- `pending`
  First failure. The record has not been retried yet.
- `retrying`
  A retry was attempted and failed again, but more retries are still allowed.
- `failed`
  Retry budget is exhausted.
- `resolved`
  The data was recovered successfully by `retry` or `replay`.

## When A Record Enters DLQ

A record is written into `dlq.ingestion` when ingestion cannot complete for a block/entity.

Typical cases:

- RPC request failed
- processor raised an exception
- downstream write path failed before the pipeline could complete

The DLQ record includes:

- `chain`
- `network`
- `pipeline`
- `entity`
- `block_number`
- `stage`
- `error_type`
- `error_message`
- `payload`
- `context`
- `retry_count`
- `max_retry`
- `status`
- `next_retry_at`

## Retry vs Replay

`retry` and `replay` both recover data, but they are not the same workflow.

### Retry

`retry` is for automatic recovery of retryable failures.

Behavior:

- consumes `dlq.ingestion`
- only considers records whose latest status is retryable
- checks retry timing and retry budget
- re-runs ingestion for the record's `block_number`
- if successful, writes a new DLQ state with `status=resolved`
- if failed again, writes a new DLQ state with `status=retrying` or `status=failed`

Use `retry` for:

- transient RPC failures
- temporary Kafka / infra issues
- intermittent upstream instability

### Replay

`replay` is for manual or batch recovery after you have already fixed the root cause.

Behavior:

- scans the latest records in `dlq.ingestion`
- filters by conditions such as `entity`, `status`, `stage`
- extracts matching `block_number` values
- re-runs the normal ingestion flow for those blocks
- if successful, writes matching DLQ records back as `status=resolved`

Use `replay` for:

- code bug fixed after bad data entered DLQ
- backfilling specific failed blocks
- controlled manual recovery in local or Kubernetes jobs

## Short State Flow

```text
ingestion failure
  -> pending

pending
  -> retry success -> resolved
  -> retry fail, retries left -> retrying
  -> retry fail, no retries left -> failed

retrying
  -> retry success -> resolved
  -> retry fail, retries left -> retrying
  -> retry fail, no retries left -> failed

pending / retrying / failed
  -> manual replay success -> resolved
```

## Operational Meaning

You can read the latest DLQ state like this:

- `pending`
  New failed record waiting for retry or manual replay
- `retrying`
  System is still attempting automatic recovery
- `failed`
  Automatic retry is done; operator action is usually required
- `resolved`
  Recovery completed successfully

## Local Commands

Read current DLQ state:

```bash
UV_CACHE_DIR=/tmp/uvcache uv run python scripts/read_dlq.py \
  --config pipeline.yaml \
  --summary \
  --pretty
```

Read only unresolved trace records:

```bash
UV_CACHE_DIR=/tmp/uvcache uv run python scripts/read_dlq.py \
  --config pipeline.yaml \
  --entity trace \
  --status pending \
  --summary \
  --pretty
```

Run one local replay:

```bash
UV_CACHE_DIR=/tmp/uvcache uv run python rpcstream/adapters/evm/jobs/dlq_replay_job.py \
  --config pipeline.yaml \
  --entity trace \
  --status pending \
  --stage processor \
  --max-records 1
```

After replay succeeds, verify the record became `resolved`:

```bash
UV_CACHE_DIR=/tmp/uvcache uv run python scripts/read_dlq.py \
  --config pipeline.yaml \
  --entity trace \
  --status resolved \
  --summary \
  --pretty
```

## EOS Behavior

If `pipeline.yaml` has:

```yaml
kafka:
  eos:
    enabled: true
```

then all Kafka write paths use EOS transactions, including:

- main raw topics
- `dlq.ingestion`
- retry result writes
- replay result writes
- resolved-state writes

If EOS is disabled in `pipeline.yaml`, all of those paths fall back to normal non-transactional writes.
