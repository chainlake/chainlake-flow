from __future__ import annotations

from prefect import flow, get_run_logger

from rpcstream.dlq_replay import run_dlq_replay


@flow(name="rpcstream-dlq-replay", log_prints=True)
async def dlq_replay_flow(
    status: str = "failed",
    entity: str | None = None,
    stage: str | None = None,
    max_records: int | None = None,
) -> None:
    """Scheduled wrapper around `rpcstream-dlq-replay` for the Prefect Cloud
    Kubernetes work pool. Replays DLQ records (default: status=failed, i.e.
    the ones rpcstream-dlq-retry already exhausted its retry budget on and
    will never touch again) through the normal engine pipeline. A cursor
    that fails again here gets re-queued as a fresh pending DLQ record
    (build_unified_dlq_record, retry_count=0) for rpcstream-dlq-retry to
    pick up -- this flow only needs to catch cases where replay itself
    couldn't run at all (Kafka/erpc unreachable), not "N of M cursors still
    broken", which is what the dashboard's DLQ section is for.
    """
    logger = get_run_logger()
    logger.info(
        "Starting DLQ replay: status=%s entity=%s stage=%s max_records=%s",
        status,
        entity,
        stage,
        max_records,
    )
    await run_dlq_replay(status=status, entity=entity, stage=stage, max_records=max_records)
    logger.info("DLQ replay finished")
