import asyncio
from typing import Set

from src.logging import log
from src.rpc_provider import AsyncRpcClient, Web3AsyncRouter, AsyncRpcScheduler, RpcErrorResult
from src import RangeResult, RangeRegistry, OrderedResultBuffer, LatestBlockTracker
from src.metrics.runtime import set_current_metrics, get_metrics
from src.metrics import MetricsContext


class IngestionEngine:
    def __init__(
        self,
        *,
        planner,
        rpc_pool,
        producer,
        submit_range_fn,
        max_inflight_ranges: int,
        rpc_max_timeout: int,
        rpc_max_inflight: int,
        metrics_context: MetricsContext,
    ):
        self.planner = planner
        self.rpc_pool = rpc_pool
        self.producer = producer
        self.submit_range = submit_range_fn
        self.max_inflight_ranges = max_inflight_ranges

        # metrics
        set_current_metrics(metrics_context)
        self.metrics = get_metrics()
        self.metrics.max_range_inflight_set(max_inflight_ranges)

        # RPC infra
        self.client = AsyncRpcClient(timeout=rpc_max_timeout)
        self.router = Web3AsyncRouter(rpc_pool, self.client)
        self.scheduler = AsyncRpcScheduler(
            router=self.router,
            max_workers=1,
            max_inflight=rpc_max_inflight,
            max_queue=rpc_max_inflight * 3,
        )

        # control plane
        self.registry = RangeRegistry()
        self.ordered_buffer = OrderedResultBuffer()
        self.inflight: Set[asyncio.Task] = set()

        # chain head tracker
        self.latest_tracker = LatestBlockTracker(self.router, refresh_interval=2.0)

    # --------------------------------------------------
    async def run(self):
        log.info("🚀 ingestion_engine_start", planner=type(self.planner).__name__)

        self.latest_tracker.start()
        await self._wait_for_chain_head()

        # initial prefill
        await self._refill_inflight()

        # -----------------------------
        # Main unified loop
        # -----------------------------
        while True:
            if not self.inflight:
                if self.planner.exhausted:
                    log.info("🏁 bounded_planner_exhausted_shutdown")
                    break

                # realtime idle wait
                await asyncio.sleep(0.2)
                await self._refill_inflight()
                continue

            done, _ = await asyncio.wait(
                self.inflight,
                return_when=asyncio.FIRST_COMPLETED,
            )

            for task in done:
                self.inflight.remove(task)
                result = await task
                await self._handle_result(result)

            await self._consume_ordered()
            await self._refill_inflight()

        await self._shutdown()

    # --------------------------------------------------
    async def _wait_for_chain_head(self):
        while True:
            latest = self.latest_tracker.get_cached()
            if latest is not None:
                return
            await asyncio.sleep(0.2)

    # --------------------------------------------------
    async def _refill_inflight(self):
        latest_block = self.latest_tracker.get_cached()
        if latest_block is None:
            return

        while len(self.inflight) < self.max_inflight_ranges:
            r = self.planner.next_range(latest_block)
            if not r:
                return

            rr = self.registry.register(
                r.range_id,
                r.start_block,
                r.end_block,
            )
            task = await self.submit_range(self.scheduler, self.registry, rr)
            self.inflight.add(task)

    # --------------------------------------------------
    async def _handle_result(self, result):
        if isinstance(result, RpcErrorResult):
            meta = result.meta
            range_id = meta.extra["range_id"]

            retry_ok = self.registry.mark_retry(
                range_id,
                error=str(result.error),
            )

            if retry_ok:
                r = self.registry.get(range_id)
                task = await self.submit_range(self.scheduler, self.registry, r)
                self.inflight.add(task)
            else:
                self.registry.mark_failed(range_id, str(result.error))
            return

        # success
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

        self.ordered_buffer.add(rr)

    # --------------------------------------------------
    async def _consume_ordered(self):
        ready = self.ordered_buffer.pop_ready()
        for rr in ready:
            await self._commit_range(rr)

    # --------------------------------------------------
    async def _commit_range(self, rr):
        """
        这里直接复用你现有的 Kafka EOS + metrics + state 写入逻辑
        """
        self.registry.mark_done(rr.range_id)

        log.info(
            "✅ range_committed",
            extra={
                "range_id": rr.range_id,
                "start": rr.start_block,
                "end": rr.end_block,
                "rpc": rr.rpc,
                "key_env": rr.key_env,
                "log_count": len(rr.logs),
            },
        )

    # --------------------------------------------------
    async def _shutdown(self):
        log.info("🧹 ingestion_engine_shutdown")
        self.producer.flush(10)
        await self.scheduler.close()
        await self.latest_tracker.stop()
