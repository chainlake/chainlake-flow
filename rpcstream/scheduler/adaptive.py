import asyncio
import contextlib
import time
from dataclasses import dataclass
from itertools import cycle
from typing import Any

import orjson
from opentelemetry import trace
from redis.asyncio import Redis

from rpcstream.rpc.models import RpcErrorResult, RpcTaskMeta
from rpcstream.scheduler.base import BaseRpcScheduler

tracer = trace.get_tracer("rpcstream.scheduler")


@dataclass(slots=True)
class SchedulerTask:
    """Payload stored in Redis ordering buffer / DLQ."""

    sequence: int
    method: str
    params: list[Any]
    retries: int = 0
    max_retries: int = 3
    metadata: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "sequence": self.sequence,
            "method": self.method,
            "params": self.params,
            "retries": self.retries,
            "max_retries": self.max_retries,
            "metadata": self.metadata or {},
        }

    @classmethod
    def from_raw(cls, raw: bytes | str) -> "SchedulerTask":
        payload = orjson.loads(raw)
        return cls(
            sequence=payload["sequence"],
            method=payload["method"],
            params=payload.get("params", []),
            retries=payload.get("retries", 0),
            max_retries=payload.get("max_retries", 3),
            metadata=payload.get("metadata", {}),
        )


class RedisOrderingBuffer:
    """FIFO buffer backed by Redis List, preserving task order by sequence."""

    def __init__(self, redis: Redis, key: str = "rpc:ordering_buffer"):
        self.redis = redis
        self.key = key

    async def push(self, task: SchedulerTask) -> None:
        await self.redis.rpush(self.key, orjson.dumps(task.to_dict()))

    async def pop(self, timeout_sec: int = 1) -> SchedulerTask | None:
        result = await self.redis.blpop(self.key, timeout=timeout_sec)
        if result is None:
            return None
        _, raw = result
        return SchedulerTask.from_raw(raw)

    async def length(self) -> int:
        return int(await self.redis.llen(self.key))


class RedisDLQ:
    """Dead-letter queue for tasks that exceed retry limit."""

    def __init__(self, redis: Redis, key: str = "rpc:dlq"):
        self.redis = redis
        self.key = key

    async def push(self, task: SchedulerTask, error: str) -> None:
        payload = task.to_dict()
        payload["error"] = error
        payload["failed_at"] = time.time()
        await self.redis.rpush(self.key, orjson.dumps(payload))

    async def pop(self, timeout_sec: int = 1) -> dict[str, Any] | None:
        result = await self.redis.blpop(self.key, timeout=timeout_sec)
        if result is None:
            return None
        _, raw = result
        return orjson.loads(raw)

    async def length(self) -> int:
        return int(await self.redis.llen(self.key))


class AdaptiveRpcScheduler(BaseRpcScheduler):
    """
    High-availability adaptive scheduler.
    - multi-client pool (round-robin + temporary quarantine on failures)
    - adaptive concurrency window
    - dynamic pause/resume based on Redis backlog
    - retries + DLQ
    """

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

            span.set_attribute("scheduler.queue_wait_ms", round(wait_ms, 2))
            span.set_attribute("scheduler.window", self.current_limit)

            try:
                client_idx = self._pick_client_idx()
                result = await self.clients[client_idx].call(task.method, task.params)

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

            except Exception as exc:
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

            finally:
                self._release_slot()

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
        mild_decrease_factor = 0.95
        strong_decrease_factor = 0.7

        if not success or latency_ms > self.latency_target_ms * self.extreme_latency_factor:
            self.current_limit = max(self.min_inflight, int(cur * strong_decrease_factor))
            return

        if latency_ms > self.latency_target_ms:
            self.current_limit = max(self.min_inflight, int(cur * mild_decrease_factor))
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
        base = super().telemetry()
        base.update(
            {
                "paused": self.paused,
                "pause_count": self.pause_count,
                "backlog": self.last_backlog,
                "dlq": self.last_dlq_length,
                "client_pool_size": len(self.clients),
            }
        )
        return base


async def main() -> None:
    """Example: produce ordered tasks, schedule RPC, consume DLQ."""

    from rpcstream.rpc.rpc_client import RpcClient

    redis = Redis(host="127.0.0.1", port=6379, password=None, decode_responses=False)
    ordering = RedisOrderingBuffer(redis, key="demo:ordering")
    dlq = RedisDLQ(redis, key="demo:dlq")

    await redis.delete("demo:ordering", "demo:dlq")

    clients = [
        RpcClient("https://eth.llamarpc.com"),
        RpcClient("https://rpc.ankr.com/eth"),
    ]

    scheduler = AdaptiveRpcScheduler(
        clients=clients,
        ordering_buffer=ordering,
        dlq=dlq,
        min_inflight=5,
        max_inflight=50,
        initial_inflight=10,
        latency_target_ms=250,
        pause_backlog_threshold=200,
        resume_backlog_threshold=50,
    )

    await scheduler.start()

    for seq in range(1, 21):
        task = SchedulerTask(
            sequence=seq,
            method="eth_blockNumber",
            params=[],
            max_retries=2,
            metadata={"source": "demo"},
        )
        await ordering.push(task)

    async def worker() -> None:
        while True:
            task = await ordering.pop(timeout_sec=1)
            if task is None:
                break
            await scheduler.submit(task)

    workers = [asyncio.create_task(worker()) for _ in range(5)]
    await asyncio.gather(*workers)

    print("scheduler telemetry:", scheduler.telemetry())
    print("ordering backlog:", await ordering.length())
    print("dlq length:", await dlq.length())

    while (item := await dlq.pop(timeout_sec=1)) is not None:
        print("dlq item:", item)

    await scheduler.stop()
    await asyncio.gather(*(client.close() for client in clients))
    await redis.close()


if __name__ == "__main__":
    asyncio.run(main())
