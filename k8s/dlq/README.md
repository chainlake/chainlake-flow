# DLQ Retry And Replay

This directory contains the Kubernetes manifests for unified DLQ operations:

- `dlq-configmap.yaml`: shared `pipeline.yaml` for DLQ workers
- `dlq-retry-deploy.yaml`: long-running retry worker
- `dlq-replay-job.yaml`: one-shot replay job

These manifests are designed for the current unified DLQ topic:

- topic: `dlq.ingestion`
- retry entrypoint: `python -m rpcstream.dlq_retry`
- replay entrypoint: `python -m rpcstream.dlq_replay`

## Prerequisites

Before using these manifests, make sure the cluster already has:

- namespace `ingestion`
- secret `kafka-credentials`
- secret `kafka-ca-secret`
- image `rpcstream:dev` available to the cluster
- Kafka topic and schema initialized via the Kafka init flow

The manifests assume the same secret keys used by the main ingestion deployment:

- `KAFKA_USERNAME`
- `KAFKA_PASSWORD`
- `KAFKA_BOOTSTRAP_SERVERS`
- `KAFAK_SCHEMA_REGISTRY`

## Files

### `dlq-configmap.yaml`

Provides `/config/pipeline.yaml` to both retry and replay workloads.

This file is the only config source used by the DLQ workloads. There is no separate retry/replay config file.

### `dlq-retry-deploy.yaml`

Runs the retry worker continuously.

Behavior:

- consumes records from `dlq.ingestion`
- checks `retry_count`, `max_retry`, and `next_retry_at`
- retries eligible records
- writes updated DLQ state back to Kafka
- marks successful retries as `resolved`

Use this as a normal always-on deployment.

### `dlq-replay-job.yaml`

Runs a one-time replay from DLQ.

Default command in the manifest:

```bash
python -m rpcstream.dlq_replay --entity transaction --status failed
```

Behavior:

- reads the unified DLQ topic
- filters records by CLI args
- rebuilds a replay block source
- re-runs the normal engine flow for matching blocks

Use this when you want manual or batch reprocessing after fixing a bug.

## Apply Order

Apply in this order:

```bash
kubectl apply -f k8s/dlq/dlq-configmap.yaml
kubectl apply -f k8s/dlq/dlq-retry-deploy.yaml
```

Replay is normally run manually when needed:

```bash
kubectl apply -f k8s/dlq/dlq-replay-job.yaml
```

## Retry Worker Operations

Deploy the retry worker:

```bash
kubectl apply -f k8s/dlq/dlq-retry-deploy.yaml
```

Check status:

```bash
kubectl -n ingestion get deploy rpcstream-dlq-retry
kubectl -n ingestion get pods -l app=rpcstream-dlq-retry
```

View logs:

```bash
kubectl -n ingestion logs deploy/rpcstream-dlq-retry -f
```

Restart after changing config:

```bash
kubectl -n ingestion rollout restart deploy/rpcstream-dlq-retry
```

Expected log examples:

- `dlq.retry_worker_started`
- `dlq.retry_succeeded`
- `dlq.retry_failed`

## Replay Job Operations

The replay manifest is intentionally a template-like job definition. Before applying it, update the `command` section if needed.

Common examples:

Replay failed transactions:

```yaml
command:
  - python
  - -m
  - rpcstream.dlq_replay
  - --entity
  - transaction
  - --status
  - failed
```

Replay failed processor-stage records:

```yaml
command:
  - python
  - -m
  - rpcstream.dlq_replay
  - --status
  - failed
  - --stage
  - processor
```

Replay only a limited batch:

```yaml
command:
  - python
  - -m
  - rpcstream.dlq_replay
  - --entity
  - block
  - --status
  - failed
  - --max-records
  - "100"
```

Run the job:

```bash
kubectl apply -f k8s/dlq/dlq-replay-job.yaml
```

Check status:

```bash
kubectl -n ingestion get jobs
kubectl -n ingestion get pods -l app=rpcstream-dlq-replay
```

View logs:

```bash
kubectl -n ingestion logs job/rpcstream-dlq-replay -f
```

Delete and rerun:

```bash
kubectl -n ingestion delete job rpcstream-dlq-replay
kubectl apply -f k8s/dlq/dlq-replay-job.yaml
```

## Customization Notes

Things you will likely customize per environment:

- `image`
- Kafka endpoints and credentials
- `pipeline.yaml` entity list
- `erpc.base_url`
- telemetry endpoints
- replay `command` arguments
- resource requests and limits

## Safety Notes

- Keep only one retry worker replica unless you intentionally redesign the coordination model.
- Replay may reprocess blocks that contain multiple entities. Use filters carefully.
- Retry and replay both depend on the unified DLQ topic carrying the latest state records.
- If you change the DLQ schema, rerun the Kafka init flow before deploying these workloads.
