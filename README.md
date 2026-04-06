# Blockchain Ingestion Engine

`blockchain-ingestion-engine` is the core ingestion engine of **Chainlake**, designed for modern blockchain data infrastructure.

It provides:

- ultra-low latency stream ingestion
- high throughput historical backfill
- ordered block processing
- async RPC concurrency
- Kafka-native downstream delivery
- multi-chain extensibility (EVM + future non-EVM)

Unlike traditional ETL-oriented blockchain extractors, `blockchain-ingestion-engine` is built for both:
* **continuous realtime semantic pipelines**
* **large-scale historical data ingestion**

# Why This Project Exists

Most existing blockchain ETL tools were designed for:
- historical export
- offline analytics
- CSV / JSON dumps
- batch pipelines

They are not optimized for:
- high throughput backfill
- async concurrency
- realtime ingestion
- Kafka-native stream processing
- semantic data freshness

`blockchain-ingestion-engine` solves both problems:
## Realtime
Low-latency block ingestion for semantic freshness.

## Historical
High-throughput backfill for full-chain reconstruction.

# Core Design Goals

## Unified Stream + Backfill Engine
Same ingestion core supports:
- realtime streaming
- historical backfill
No duplicated code path.

## Stream First but Batch Strong
- Realtime guarantees freshness.
- Backfill guarantees full-chain completeness.
Both are first-class citizens.

## Ordered Output Guarantee

Even under async concurrency:
- block ordering preserved
- downstream deterministic sequence maintained

## Horizontal Scalability
Can scale by:
- block partition
- chain partition
- topic partition

## Multi-Chain Future

Supports:
### EVM now
Examples:
- Ethereum
- BNB Chain
- Polygon

### Non-EVM future:
Planned:
- Sui
- Aptos
- Solana

# High-Level Architecture
```mermaid
flowchart TD

A[Blockchain RPC Nodes] --> B[eRPC / RPC Adapter Layer]

B --> C[Async Fetch Scheduler]

C --> D[Block Fetch Workers]

D --> E[Decode Pipeline]

E --> F[Ordering Buffer]

F --> G[Async Kafka Producer]

G --> H[Kafka Topics]

H --> I[Realtime Semantic Layer]

H --> J[Batch Compute Layer]
```

# Architecture V3 (Production Model)

```mermaid
flowchart TD

subgraph RPC
A1[EVM RPC via eRPC]
A2[Non-EVM RPC via Adapter]
end

subgraph Ingestion
B1[Async Scheduler]
B2[Concurrent Fetch Workers]
B3[Retry / Timeout / Backpressure]
end

subgraph Decode
C1[Block Decoder]
C2[Transaction Decoder]
C3[Log Decoder]
end

subgraph Ordering
D1[Sequence Buffer]
D2[Gap Recovery]
end

subgraph Delivery
E1[Async Kafka Producer]
E2[Partition Routing]
end

subgraph Downstream
F1[Realtime Semantic Storage]
F2[Historical Lakehouse]
end

A1 --> B1
A2 --> B1

B1 --> B2
B2 --> B3

B3 --> C1
C1 --> C2
C2 --> C3

C3 --> D1
D1 --> D2

D2 --> E1
E1 --> E2

E2 --> F1
E2 --> F2
```

# Project Structure
```text
blockchain-ingestion-engine/

├── cmd/
│   └── cli.py

├── configs/
│   ├── evm.yaml
│   ├── bsc.yaml
│   └── sui.yaml

├── blockchain_ingestion/

│   ├── core/
│   │   ├── scheduler.py
│   │   ├── fetcher.py
│   │   ├── retry.py
│   │   ├── ordering.py
│   │   └── checkpoint.py

│   ├── rpc/
│   │   ├── evm/
│   │   ├── sui/
│   │   └── adapter_base.py

│   ├── decoder/
│   │   ├── block_decoder.py
│   │   ├── tx_decoder.py
│   │   └── log_decoder.py

│   ├── producer/
│   │   ├── kafka_async.py
│   │   └── partitioner.py

│   ├── state/
│   │   ├── redis_buffer.py
│   │   └── sequence_state.py

│   ├── metrics/
│   │   ├── prometheus.py
│   │   └── tracing.py

│   └── utils/

├── tests/

├── docker/

├── deploy/
│   ├── k8s/
│   └── helm/

├── scripts/

└── README.md
```

# Core Features

## Async RPC Fetching
- high concurrency
- adaptive worker pools
- latency aware

## Ordered Block Delivery
- sequence buffer
- gap recovery

## Async Kafka Producer
- high throughput
- partition aware
- non-blocking delivery

## RPC Reliability via eRPC
Uses:
- timeout
- retry
- circuit breaker
- provider failover

# Key Difference from Traditional ETL
Traditional ETL:
- export-oriented
- offline-first
- synchronous RPC
- limited throughput

Chainlake Ingestion Engine:
- async-first
- stream-native
- backfill-optimized
- Kafka-ready
- horizontally scalable

# Planned Features
## Redis Ordering Buffer
for large-scale strict ordering

## Multi-region RPC routing
## Adaptive concurrency controller
## chain-specific parser plugins

---

# Supported Modes

## Realtime Stream Mode

Optimized for:

- latest block ingestion
- low-latency semantic updates

Example Run:
```bash
python cmd/cli.py \
  --chain bsc \
  --mode stream \
  --start-block latest
```
Output Topics:
Examples:
```text
blocks
transactions
logs
erc20_transfers
erc721_transfers
erc1155_transfers
```

## Batch Backfill Mode

Optimized for:

- large historical range export
- maximum throughput

# Downstream Integration

Designed for:
- Apache Kafka
- Apache Spark
- Apache Iceberg
- ClickHouse

# realtime semantic freshness

Without low-latency ingestion, semantic layer always lags.

This project ensures:
- semantic freshness
- low compute delay
- scalable chain expansion

# Future Roadmap

## v0.1 (WIP)
- EVM stable runtime
- Kafka sink
- ordered buffer

## v0.2
- Prometheus metrics
- OpenTelemetry traces
- Loki logs

## v0.3
- Parquet / Iceberg sink
- replay state persistence

## v0.4
- Redis ordering
- non-EVM adapter framework

## v0.5
- unified semantic ingestion engine
- exactly-once end-to-end delivery

## v1.0
- production multi-chain release

---

# Positioning

Blockchain Ingestion Engine is not a traditional ETL exporter.

It is designed as:

> blockchain Ingestion runtime infrastructure

for modern semantic data systems.

---

# License

Apache-2.0
