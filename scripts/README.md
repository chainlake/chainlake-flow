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

## Verify Trace DLQ Write

`verify_trace_dlq.py` triggers one synthetic `trace` processor failure and then
reads `dlq.ingestion` back to confirm the record was actually written.

Run with the default pipeline config:

```bash
UV_CACHE_DIR=/tmp/uvcache uv run python scripts/verify_trace_dlq.py
```

Use a specific pipeline config:

```bash
UV_CACHE_DIR=/tmp/uvcache uv run python scripts/verify_trace_dlq.py \
  --config rpcstream/pipeline.yaml
```

Use a fixed synthetic block number:

```bash
UV_CACHE_DIR=/tmp/uvcache uv run python scripts/verify_trace_dlq.py \
  --block-number 95281318
```

On success the script prints one JSON object containing `verified: true`, the
DLQ topic name, the synthetic block number, and the matching error fields.