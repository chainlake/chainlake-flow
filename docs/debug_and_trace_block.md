## trace_block vs debug_traceBlockByNumber

| Source                     | Client Type         | Behavior             |
| -------------------------- | ------------------- | -------------------- |
| `trace_block`              | Parity/OpenEthereum | filtered, normalized |
| `debug_traceBlockByNumber` | Geth                | full execution trace |


## Production Implication
| Use Case                          | Prefer                |
| --------------------------------- | --------------------- |
| analytics / dashboards            | trace_block           |
| deep debugging / MEV / call graph | debug_trace           |
| unified system                    | BOTH (with tagging)   |
