import asyncio
import json
from typing import Set

from confluent_kafka.serialization import SerializationContext, MessageField

from src.logging import log
from src.rpc_provider import (
    Web3AsyncRouter,
    AsyncRpcClient,
    AsyncRpcScheduler,
    RpcErrorResult,
)
from src import (
    RangeResult,
    RangeRegistry,
    OrderedResultBuffer,
    LatestBlockTracker,
)
from src.metrics import MetricsContext
from src.metrics.runtime import set_current_metrics, get_metrics
from src.web3_utils import current_utctime

# submit_range 仍然复用你原来的

async def submit_range(scheduler, registry, r):
    task = asyncio.create_task(
        scheduler.submit(
            "eth_getLogs",
            [{
                "fromBlock": hex(r.start_block),
                "toBlock": hex(r.end_block),
                # Optional: "topics": [...], "address" : [...]
            }],
            meta={
                "range_id": r.range_id,
                "start_block": r.start_block,
                "end_block": r.end_block,
                "retry": r.retry,
            },
        )
    )
    registry.mark_inflight(r.range_id, task_id=id(task))
    return task

async def stream_ingest_logs(
    *,
    planner,
    rpc_pool,
    producer,
    blocks_topic: str,
    state_topic: str,
    blocks_value_serializer,
    state_value_serializer,
    job_name: str,
    run_id: str,
    max_inflight_ranges: int,
    rpc_timeout: int,
    rpc_max_inflight: int,
):
    """
    Unified streaming ingestion engine.

    Supports:
    - Realtime (TailingRangePlanner)
    - Resume / Backfill (BoundedRangePlanner)

    Planner controls:
    - whether ranges are bounded
    - when exhaustion happens
    """

    # -----------------------------
    # Metrics
    # -----------------------------
    metrics_context = MetricsContext.from_env()
    set_current_metrics(metrics_context)
    metrics = get_metrics()

    metrics.max_range_inflight_set(max_inflight_ranges)

    planner_exhausted = False

    # -----------------------------
    # RPC infra
    # -----------------------------
    client = AsyncRpcClient(timeout=rpc_timeout)
    router = Web3AsyncRouter(rpc_pool, client)

    scheduler = AsyncRpcScheduler(
        router=router,
        max_workers=1,
        max_inflight=rpc_max_inflight,
        max_queue=rpc_max_inflight * 3,
    )

    # -----------------------------
    # Control plane
    # -----------------------------
    registry = RangeRegistry()
    ordered_buffer = OrderedResultBuffer()
    inflight: Set[asyncio.Task] = set()

    # -----------------------------
    # Latest block tracker
    # -----------------------------
    latest_tracker = LatestBlockTracker(router, refresh_interval=2.0)
    latest_tracker.start()

    # 等待 first latest block
    while True:
        latest_block = latest_tracker.get_cached()
        if latest_block is not None:
            break
        await asyncio.sleep(0.2)

    # -----------------------------
    # Pre-fill inflight window
    # -----------------------------
    while len(inflight) < max_inflight_ranges:
        r = planner.next_range(latest_block)
        if not r:
            if planner.exhausted:
                planner_exhausted = True
            break

        rr = registry.register(r.range_id, r.start_block, r.end_block)
        inflight.add(await submit_range(scheduler, registry, rr))

    # -----------------------------
    # Main streaming loop
    # -----------------------------
    while inflight or not planner_exhausted:
        if not inflight:
            # realtime idle waiting
            await asyncio.sleep(0.2)
            latest_block = latest_tracker.get_cached()
            continue

        done, _ = await asyncio.wait(
            inflight,
            return_when=asyncio.FIRST_COMPLETED,
        )

        for task in done:
            inflight.remove(task)
            result = await task

            # -----------------------------
            # RPC error
            # -----------------------------
            if isinstance(result, RpcErrorResult):
                meta = result.meta
                range_id = meta.extra["range_id"]

                retry_ok = registry.mark_retry(
                    range_id,
                    error=str(result.error),
                )

                if retry_ok:
                    r = registry.get(range_id)
                    inflight.add(await submit_range(scheduler, registry, r))
                else:
                    registry.mark_failed(range_id, str(result.error))

                continue

            # -----------------------------
            # Success path
            # -----------------------------
            logs, rpc, key_env, trace, wid, meta = result

            rr = RangeResult(
                range_id=meta.extra["range_id"],
                start_block=meta.extra["start_block"],
                end_block=meta.extra["end_block"],
                logs=logs or [],
                rpc=rpc,
                key_env=key_env,
                task_id=meta.task_id,
            )

            ordered_buffer.add(rr)

            ready_ranges = ordered_buffer.pop_ready()

            for rr in ready_ranges:
                producer.begin_transaction()

                logs_by_block = {}
                for log_item in rr.logs:
                    bn = log_item.get("blockNumber")
                    if bn is None:
                        continue
                    if isinstance(bn, str):
                        bn = int(bn, 16)
                    logs_by_block.setdefault(bn, []).append(log_item)

                for bn, txs in logs_by_block.items():
                    total_tx = len(txs)

                    metrics.block_processed.inc()
                    metrics.tx_processed.inc(total_tx)

                    for idx, tx in enumerate(txs):
                        tx_record = {
                            "block_height": bn,
                            "job_name": job_name,
                            "run_id": run_id,
                            "inserted_at": current_utctime(),
                            "raw": json.dumps(tx),
                            "tx_index": idx,
                        }

                        producer.produce(
                            topic=blocks_topic,
                            key=f"{bn}-{idx}",
                            value=blocks_value_serializer(
                                tx_record,
                                SerializationContext(
                                    blocks_topic, MessageField.VALUE
                                ),
                            ),
                        )
                    producer.poll(0)

                # -----------------------------
                # Commit checkpoint (range-level)
                # -----------------------------
                last_committed_block = rr.end_block

                state_record = {
                    "job_name": job_name,
                    "run_id": f"{run_id}-task-{rr.task_id}-range-{rr.range_id}",
                    "range": {
                        "start": rr.start_block,
                        "end": rr.end_block,
                    },
                    "checkpoint": last_committed_block,
                    "status": "running",
                    "inserted_at": current_utctime(),
                }

                producer.produce(
                    state_topic,
                    key=job_name,
                    value=state_value_serializer(
                        state_record,
                        SerializationContext(
                            state_topic, MessageField.VALUE
                        ),
                    ),
                )
                producer.poll(0)
                producer.commit_transaction()

                registry.mark_done(rr.range_id)

                log.info(
                    "✅ range_committed",
                    extra={
                        "task_id": rr.task_id,
                        "range_id": rr.range_id,
                        "start": rr.start_block,
                        "end": rr.end_block,
                        "rpc": rr.rpc,
                        "key_env": rr.key_env,
                        "log_count": len(rr.logs),
                    },
                )

                metrics.tx_per_block.observe(total_tx)
                metrics.block_committed.inc()
                metrics.tx_committed.inc(total_tx)

                latest_block = latest_tracker.get_cached()
                if latest_block is not None:
                    metrics.chain_latest_block.set(latest_block)
                    metrics.checkpoint_block.set(last_committed_block)
                    metrics.checkpoint_lag.set(
                        max(0, latest_block - last_committed_block)
                    )

        # -----------------------------
        # Refill inflight window
        # -----------------------------
        latest_block = latest_tracker.get_cached()
        if latest_block is not None:
            while len(inflight) < max_inflight_ranges:
                r = planner.next_range(latest_block)
                if not r:
                    if planner.exhausted:
                        planner_exhausted = True
                    break

                rr = registry.register(
                    r.range_id,
                    r.start_block,
                    r.end_block,
                )
                inflight.add(await submit_range(scheduler, registry, rr))

        if planner_exhausted and not inflight:
            log.info("🏁 planner_exhausted_shutdown")
            break

    # -----------------------------
    # Graceful shutdown
    # -----------------------------
    producer.flush(10)
    await scheduler.close()
    await latest_tracker.stop()
