


## pipeline vs entity
> pipeline : fetch + process
> entity: produce


Key design principle
```bash
❌ Pipeline should NOT depend on entity list
✅ Processor decides what entities are available
✅ Config decides what to KEEP
```

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
    "sol": ["slot", "transaction", "instruction"]
}
```

## Pipeline vs Entity vs Adapter (final model)
```
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


## multi-pipeline orchestration (block + log + trace running together without duplication)

high-level flow
```text
           BlockSource
                ↓
        (block_number stream)
                ↓
            Fetcher (ONE)
                ↓
        Raw Block Data (shared)
                ↓
        ┌────────┬────────┬────────┐
        ↓        ↓        ↓        ↓
   BlockProc  TxProc   LogProc  TraceProc
        ↓        ↓        ↓        ↓
     Kafka    Kafka    Kafka    Kafka
```

from
```
block pipeline → fetch block
tx pipeline → fetch block AGAIN ❌
```

to
```
fetch block → reuse everywhere ✅
```


### Pipeline = configuration, not code

```yaml
pipeline:
  type: "evm_full"   # logical grouping

schema:
  entities: ["block", "transaction", "receipt", "log", "trace"]
```