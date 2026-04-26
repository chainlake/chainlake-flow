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
