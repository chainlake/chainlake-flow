import asyncio
from collections.abc import Awaitable, Callable

from blockchain_ingestion.execution.ordered_buffer import OrderedResultBuffer
from blockchain_ingestion.execution.result_merger import RangeResult
from blockchain_ingestion.planner.stream_cursor import LatestBlockTracker
from blockchain_ingestion.rpc.erpc_client import ErpcScheduler, RpcErrorResult
from blockchain_ingestion.state.range_registry import RangeRegistry
from blockchain_ingestion.utils.logging import get_logger

logger = get_logger(__name__)

SubmitRangeFn = Callable[[ErpcScheduler, RangeRegistry, object], Awaitable[asyncio.Task]]
CommitFn = Callable[[RangeResult], Awaitable[None]]


class IngestionEngine:
    def __init__(
        self,
        *,
        planner,
        scheduler: ErpcScheduler,
        tracker: LatestBlockTracker,
        submit_range_fn: SubmitRangeFn,
        on_commit: CommitFn,
        max_inflight_ranges: int,
    ):
        self.planner = planner
        self.scheduler = scheduler
        self.tracker = tracker
        self.submit_range_fn = submit_range_fn
        self.on_commit = on_commit
        self.max_inflight_ranges = max_inflight_ranges

        self.registry = RangeRegistry()
        self.ordered_buffer = OrderedResultBuffer()
        self.inflight: set[asyncio.Task] = set()

    async def run(self) -> None:
        self.tracker.start()
        await self._wait_chain_head()
        await self._refill_inflight()

        while True:
            if not self.inflight:
                if self.planner.exhausted:
                    logger.info("planner exhausted, shutdown")
                    break
                await asyncio.sleep(0.2)
                await self._refill_inflight()
                continue

            done, _ = await asyncio.wait(self.inflight, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                self.inflight.remove(task)
                result = await task
                await self._handle_task_result(result)

            for ready in self.ordered_buffer.pop_ready():
                await self.on_commit(ready)
                self.registry.mark_done(ready.range_id)

            await self._refill_inflight()

        await self.tracker.stop()

    async def _wait_chain_head(self) -> None:
        while self.tracker.get_cached() is None:
            await asyncio.sleep(0.2)

    async def _refill_inflight(self) -> None:
        latest = self.tracker.get_cached()
        if latest is None:
            return

        while len(self.inflight) < self.max_inflight_ranges:
            block_range = self.planner.next_range(latest)
            if not block_range:
                return
            record = self.registry.register(
                block_range.range_id,
                block_range.start_block,
                block_range.end_block,
            )
            task = await self.submit_range_fn(self.scheduler, self.registry, record)
            self.inflight.add(task)

    async def _handle_task_result(self, task_result: object) -> None:
        if isinstance(task_result, RpcErrorResult):
            range_id = task_result.meta.extra["range_id"]
            if self.registry.mark_retry(range_id, str(task_result.error)):
                record = self.registry.get(range_id)
                task = await self.submit_range_fn(self.scheduler, self.registry, record)
                self.inflight.add(task)
            else:
                self.registry.mark_failed(range_id, str(task_result.error))
            return

        payload, meta = task_result
        range_result = RangeResult(
            range_id=meta.extra["range_id"],
            start_block=meta.extra["start_block"],
            end_block=meta.extra["end_block"],
            payload=payload,
            meta=meta,
        )
        self.ordered_buffer.add(range_result)
