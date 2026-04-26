# Scripts

Local operational scripts for `chainlake-flow`.

## Aiven Kafka ACLs

`aiven_kafka_acls.py` creates Kafka-native ACLs for the rpcstream Kafka sink by
reading the same runtime configuration as the application.

Inputs:

- `rpcstream/pipeline.yaml` for pipeline name, entities, topics, checkpoint topic,
  and Kafka EOS `transactional.id`.
- `.env` for Kafka and Aiven credentials.

Required `.env` variables:

```bash
KAFKA_USERNAME=avnadmin
AIVEN_TOKEN=...
AIVEN_PROJECT=datakube
AIVEN_SERVICE=kafka-default
```

The script uses the project config loader, so the same `.env` file can also
provide the normal Kafka client variables such as `KAFKA_BOOTSTRAP_SERVERS`,
`KAFKA_PASSWORD`, `KAFKA_SECURITY_PROTOCOL`, `KAFKA_SASL_MECHANISM`, and
`KAFKA_CA_PATH`.

Preview ACLs without changing Aiven:

```bash
UV_CACHE_DIR=/tmp/uvcache uv run python scripts/aiven_kafka_acls.py
```

Create missing ACLs:

```bash
UV_CACHE_DIR=/tmp/uvcache uv run python scripts/aiven_kafka_acls.py --apply
```

Use a different pipeline config:

```bash
UV_CACHE_DIR=/tmp/uvcache uv run python scripts/aiven_kafka_acls.py \
  --config rpcstream/pipeline.yaml \
  --apply
```

The script creates Kafka-native ACLs for:

- `TransactionalId` when Kafka EOS is enabled.
- `Cluster` `IdempotentWrite` when Kafka EOS is enabled.
- Main sink topics derived from `kafka.common.topic_template` and `entities`.
- The unified DLQ topic.
- The checkpoint topic and `checkpoint-loader-` consumer group when checkpointing is enabled.

Notes:

- This is intentionally Aiven API based, not Kafka Admin API based. Kafka client
  credentials often cannot manage ACLs even when they can read and write topics.
- Kafka EOS can still fail on Aiven plans that do not support the transaction
  coordinator/internal transaction state requirements. If ACLs exist but
  `init_transactions()` still times out, verify the Aiven Kafka plan and broker
  count before changing application code.

## Read Unified DLQ

`read_dlq.py` reads `dlq.ingestion` and decodes the protobuf value using the
project Schema Registry settings. This is useful because Aiven's topic browser
shows protobuf payloads as binary text unless it is using a schema-aware decoder.

Read up to 20 records from the beginning with a one-off consumer group:

```bash
UV_CACHE_DIR=/tmp/uvcache uv run python scripts/read_dlq.py --pretty
```

Read only failed block records:

```bash
UV_CACHE_DIR=/tmp/uvcache uv run python scripts/read_dlq.py \
  --status failed \
  --entity block \
  --pretty
```

Print compact summaries without full payload/context:

```bash
UV_CACHE_DIR=/tmp/uvcache uv run python scripts/read_dlq.py \
  --summary \
  --max-records 10
```

Read new records only:

```bash
UV_CACHE_DIR=/tmp/uvcache uv run python scripts/read_dlq.py \
  --offset latest \
  --timeout-sec 60
```

Use a stable consumer group and commit offsets:

```bash
UV_CACHE_DIR=/tmp/uvcache uv run python scripts/read_dlq.py \
  --group-id rpcstream-dlq-debug \
  --commit
```

The script prints decoded records to stdout as JSON. Consumer metadata and the
final scanned/emitted counters are printed to stderr.

## Compact Existing DLQ Payloads

`compact_dlq_payloads.py` rewrites existing `dlq.ingestion` records so the
`payload` field contains only the same summary shape used by new DLQ writes.
This is useful after switching away from full raw payload storage.

Dry-run first:

```bash
UV_CACHE_DIR=/tmp/uvcache uv run python scripts/compact_dlq_payloads.py
```

Apply the migration:

```bash
UV_CACHE_DIR=/tmp/uvcache uv run python scripts/compact_dlq_payloads.py --apply --yes
```

Use an explicit snapshot path:

```bash
UV_CACHE_DIR=/tmp/uvcache uv run python scripts/compact_dlq_payloads.py \
  --snapshot-file /tmp/dlq_payload_snapshot.jsonl \
  --apply \
  --yes
```

Restore records from a snapshot file:

```bash
UV_CACHE_DIR=/tmp/uvcache uv run python scripts/compact_dlq_payloads.py \
  --restore-from /tmp/dlq_payload_snapshot.jsonl \
  --apply
```

The apply flow:

1. Reads all records up to the current topic high watermarks.
2. Converts `payload` to a compact summary.
3. Writes the converted records to a local JSONL snapshot.
4. Deletes records before the captured high watermarks.
5. Republishes the converted records to the same partitions.

Pause pipeline writers before running this migration when possible. Records
written after the snapshot high watermarks are not deleted, but replay/debug
ordering is easier to reason about when the topic is idle.
