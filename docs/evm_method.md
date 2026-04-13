
eth_getBlockByNumber → get (number, hash)

version_id = block_hash

transactions/logs/receipts/traces payload:
```text
{
  "block_hash": block_hash,
  "version_id: version_id
}
```

## Principle
Reorg handling should happen at the first stateful storage layer (ClickHouse)

## Unified abstraction
finality_model = {
  "btc": probabilistic,
  "evm": probabilistic+finality_delay,
  "solana": fast-finality-with-rollbacks,
  "sui": deterministic
}