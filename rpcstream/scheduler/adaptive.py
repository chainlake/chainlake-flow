import asyncio
import contextlib
import time
from itertools import cycle
from typing import Any

from opentelemetry import trace

from rpcstream.rpc.models import RpcErrorResult, RpcTaskMeta
from rpcstream.runtime.redis_queue import RedisDLQ, RedisOrderingBuffer
from rpcstream.scheduler.base import BaseRpcScheduler
from rpcstream.scheduler.models import SchedulerTask

tracer = trace.get_tracer("rpcstream.scheduler")


class AdaptiveRpcScheduler(BaseRpcScheduler):
    """Adaptive high-availability scheduler with Redis backpressure + DLQ."""

    def __init__(
        self,
        clients: list[Any],
        ordering_buffer: RedisOrderingBuffer,
        dlq: RedisDLQ,
        *,
        pause_backlog_threshold: int = 10_000,
        resume_backlog_threshold: int = 2_000,
        extreme_latency_factor: float = 4.0,
        client_cooldown_sec: int = 15,
        **kwargs: Any,
    ):
        super().__init__(**kwargs)
        if not clients:
            raise ValueError("AdaptiveRpcScheduler requires at least one RpcClient")

        self.clients = clients
        self._client_cycle = cycle(range(len(clients)))
        self._client_quarantine_until = [0.0 for _ in clients]

        self.ordering_buffer = ordering_buffer
        self.dlq = dlq
        self.pause_backlog_threshold = pause_backlog_threshold
        self.resume_backlog_threshold = resume_backlog_threshold
        self.extreme_latency_factor = extreme_latency_factor
        self.client_cooldown_sec = client_cooldown_sec

        self.paused = False
        self.pause_count = 0
        self.last_backlog = 0
        self.last_dlq_length = 0
        self._backpressure_task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        if self._backpressure_task is None:
            self._backpressure_task = asyncio.create_task(self._backpressure_loop())

    async def stop(self) -> None:
        if self._backpressure_task is not None:
            self._backpressure_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._backpressure_task
            self._backpressure_task = None

    async def submit(self, task: SchedulerTask) -> Any | RpcErrorResult:
        enqueue_ts = time.time()

        with tracer.start_as_current_span("scheduler.submit") as span:
            span.set_attribute("rpc.method", task.method)
            span.set_attribute("task.sequence", task.sequence)

            while self.paused:
                await asyncio.sleep(0.05)

            await self._acquire_slot()
            wait_ms = (time.time() - enqueue_ts) * 1000
            self._update_queue_wait(wait_ms)
            submit_ts = time.time()

            meta = RpcTaskMeta(
                task_id=id(asyncio.current_task()),
                submit_ts=submit_ts,
                extra={
                    "sequence": task.sequence,
                    "queue_wait_ms": round(wait_ms, 2),
                    "retries": task.retries,
                    **(task.metadata or {}),
                },
            )

            client_idx = self._pick_client_idx()
            span.set_attribute("scheduler.window", self.current_limit)

            try:
                result = await self.clients[client_idx].call(task.method, task.params)
                return self._handle_success(result, meta, client_idx, submit_ts, span)
            except Exception as exc:
                return await self._handle_error(exc, task, meta, client_idx, submit_ts, span)
            finally:
                self._release_slot()

    def _handle_success(
        self,
        result: Any,
        meta: RpcTaskMeta,
        client_idx: int,
        submit_ts: float,
        span: Any,
    ) -> tuple[Any, RpcTaskMeta]:
        latency = (time.time() - submit_ts) * 1000
        self.success += 1
        self._update_latency(latency)
        self._adjust_window(success=True, latency_ms=latency)

        meta.extra["latency_ms"] = round(latency, 2)
        meta.extra["client_idx"] = client_idx

        span.set_attribute("scheduler.status", "ok")
        span.set_attribute("scheduler.client_idx", client_idx)
        span.set_attribute("scheduler.latency_ms", round(latency, 2))
        return result, meta

    async def _handle_error(
        self,
        exc: Exception,
        task: SchedulerTask,
        meta: RpcTaskMeta,
        client_idx: int,
        submit_ts: float,
        span: Any,
    ) -> RpcErrorResult:
        latency = (time.time() - submit_ts) * 1000
        self.errors += 1
        self._update_latency(latency)
        self._adjust_window(success=False, latency_ms=latency)

        task.retries += 1
        self._mark_client_unhealthy(client_idx)

        if task.retries > task.max_retries:
            await self.dlq.push(task, str(exc))
        else:
            await self.ordering_buffer.push(task)

        span.set_attribute("scheduler.status", "error")
        span.set_attribute("scheduler.exception", str(exc))
        span.set_attribute("scheduler.latency_ms", round(latency, 2))
        span.set_attribute("task.retries", task.retries)

        return RpcErrorResult(exc, meta)

    def _pick_client_idx(self) -> int:
        now = time.time()
        for _ in range(len(self.clients)):
            idx = next(self._client_cycle)
            if now >= self._client_quarantine_until[idx]:
                return idx
        return next(self._client_cycle)

    def _mark_client_unhealthy(self, idx: int) -> None:
        self._client_quarantine_until[idx] = time.time() + self.client_cooldown_sec

    def _adjust_window(self, success: bool, latency_ms: float) -> None:
        cur = self.current_limit

        if not success or latency_ms > self.latency_target_ms * self.extreme_latency_factor:
            self.current_limit = max(self.min_inflight, int(cur * 0.7))
            return

        if latency_ms > self.latency_target_ms:
            self.current_limit = max(self.min_inflight, int(cur * 0.95))
        else:
            self.current_limit = min(self.max_inflight, cur + 1)

    async def _backpressure_loop(self) -> None:
        while True:
            with tracer.start_as_current_span("scheduler.backpressure") as span:
                backlog = await self.ordering_buffer.length()
                dlq_length = await self.dlq.length()

                self.last_backlog = backlog
                self.last_dlq_length = dlq_length

                span.set_attribute("scheduler.backlog", backlog)
                span.set_attribute("scheduler.dlq", dlq_length)

                if backlog >= self.pause_backlog_threshold:
                    if not self.paused:
                        self.pause_count += 1
                    self.paused = True
                    self.current_limit = max(self.min_inflight, int(self.current_limit * 0.7))
                elif backlog <= self.resume_backlog_threshold:
                    self.paused = False

            await asyncio.sleep(1.0)

    def telemetry(self) -> dict[str, Any]:
        data = super().telemetry()
        data.update(
            {
                "paused": self.paused,
                "pause_count": self.pause_count,
                "backlog": self.last_backlog,
                "dlq": self.last_dlq_length,
                "client_pool_size": len(self.clients),
            }
        )
        return data
