
# Pipeline notes

## Pipeline vs entity

- pipeline: fetch + process
- entity: produce

### Design principle

- the pipeline should not depend on the entity list
- the processor decides which entities are available
- the config decides what to keep

```yaml
pipeline:
  type: "block"

schema:
  entities:
    - block
    - transaction
```

```python
ENTITY_REGISTRY = {
    "evm": ["block", "transaction", "receipt", "log", "trace"],
    "sui": ["checkpoint", "transaction", "event"],
    "sol": ["slot", "transaction", "instruction"],
}
```

## Pipeline vs entity vs adapter

```text
Adapter (EVM / SUI / SOL)
        ↓
Pipeline (block / log / trace)
        ↓
Processor
        ↓
Entities (block / tx / log ...)
        ↓
Engine filter (config-driven)
        ↓
Kafka topics
```

```python
ENTITY_META = {
    "block": {"pk": "block_number"},
    "transaction": {"pk": "tx_hash"},
    "log": {"pk": "log_id"},
}
```

## Multi-pipeline orchestration

The block, log, and trace paths share the same block fetch when they run
together:

```text
BlockSource
    ↓
(block_number stream)
    ↓
Fetcher (one)
    ↓
Raw block data (shared)
    ↓
┌────────┬────────┬────────┐
↓        ↓        ↓        ↓
Block    Tx       Log      Trace
proc     proc     proc     proc
    ↓        ↓        ↓        ↓
  Kafka    Kafka    Kafka    Kafka
```

That avoids fetching the same block more than once for the same cursor.

```yaml
pipeline:
  type: "evm_full"

schema:
  entities: ["block", "transaction", "receipt", "log", "trace"]
```
