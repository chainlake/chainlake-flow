# Kubernetes DLQ

This directory contains Kubernetes manifests for DLQ operations.

For DLQ semantics, statuses, retry vs replay behavior, and EOS rules, use:

- [docs/dlq.md](/home/ubuntu/repos/chainlake-flow/docs/dlq.md:1)

This README only covers Kubernetes deployment and execution.

## Files

- `dlq-configmap.yaml`
  Provides `/config/pipeline.yaml` to DLQ workloads.
- `dlq-retry-deploy.yaml`
  Runs the long-lived retry worker.
- `dlq-replay-job.yaml`
  Runs a one-shot replay job.

## Entrypoints

- retry:
  `python -m rpcstream.dlq_retry`
- replay:
  `python -m rpcstream.adapters.evm.jobs.dlq_replay_job run`

## Prerequisites

Cluster prerequisites:

- namespace `ingestion`
- secret `kafka-credentials`
- secret `kafka-ca-secret`
- image `rpcstream:dev` available to the cluster
- Kafka topics and schema already initialized

Expected secret keys:

- `KAFKA_USERNAME`
- `KAFKA_PASSWORD`
- `KAFKA_BOOTSTRAP_SERVERS`
- `KAFAK_SCHEMA_REGISTRY`

## Apply Order

Apply shared config and retry worker first:

```bash
kubectl apply -f k8s/dlq/dlq-configmap.yaml
kubectl apply -f k8s/dlq/dlq-retry-deploy.yaml
```

Run replay manually when needed:

```bash
kubectl apply -f k8s/dlq/dlq-replay-job.yaml
```

## Retry Worker

Deploy:

```bash
kubectl apply -f k8s/dlq/dlq-retry-deploy.yaml
```

Check:

```bash
kubectl -n ingestion get deploy rpcstream-dlq-retry
kubectl -n ingestion get pods -l app=rpcstream-dlq-retry
```

Logs:

```bash
kubectl -n ingestion logs deploy/rpcstream-dlq-retry -f
```

Restart after config change:

```bash
kubectl -n ingestion rollout restart deploy/rpcstream-dlq-retry
```

## Replay Job

The replay job is driven by environment variables in `dlq-replay-job.yaml`.

Common filters:

- `DLQ_REPLAY_ENTITY`
- `DLQ_REPLAY_STATUS`
- `DLQ_REPLAY_STAGE`
- `DLQ_REPLAY_MAX_RECORDS`
- `DLQ_REPLAY_GROUP_ID`
- `DLQ_REPLAY_OFFSET`

Default example in the manifest:

- `DLQ_REPLAY_ENTITY=transaction`
- `DLQ_REPLAY_STATUS=failed`
- `DLQ_REPLAY_OFFSET=earliest`

Run:

```bash
kubectl apply -f k8s/dlq/dlq-replay-job.yaml
```

Check:

```bash
kubectl -n ingestion get jobs
kubectl -n ingestion get pods -l app=rpcstream-dlq-replay
```

Logs:

```bash
kubectl -n ingestion logs job/rpcstream-dlq-replay -f
```

Delete and rerun:

```bash
kubectl -n ingestion delete job rpcstream-dlq-replay
kubectl apply -f k8s/dlq/dlq-replay-job.yaml
```

## Render A Replay Job

Instead of editing YAML manually, render the replay job from `pipeline.yaml`:

```bash
UV_CACHE_DIR=/tmp/uvcache uv run python -m rpcstream.adapters.evm.jobs.dlq_replay_job render \
  --config pipeline.yaml \
  --entity trace \
  --status pending \
  --stage processor \
  --max-records 1 \
  --output k8s/dlq/dlq-replay-job.yaml
```

Then apply it:

```bash
kubectl apply -f k8s/dlq/dlq-replay-job.yaml
```

## Notes

- Keep retry worker at one replica unless coordination is redesigned.
- Replay works at block granularity, not single-event granularity.
- Both retry and replay read latest DLQ state from `dlq.ingestion`.
- If `pipeline.yaml` has `kafka.eos.enabled=true`, DLQ Kubernetes flows also use EOS writes.
