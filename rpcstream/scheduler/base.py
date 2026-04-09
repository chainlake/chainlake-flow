import asyncio
import time

from opentelemetry import trace

tracer = trace.get_tracer("rpcstream.scheduler")


class BaseScheduler:
    """
    Base scheduler:
    - adaptive logical window
    - semaphore as hard cap
    - EMA telemetry
    """

    def __init__(
        self,
        min_inflight=5,
        max_inflight=50,
        initial_inflight=10,
        latency_target_ms=200,
    ):
        self.min_inflight = min_inflight
        self.max_inflight = max_inflight
        self.current_limit = initial_inflight
        self.latency_target_ms = latency_target_ms

        # hard cap only
        self.sem = asyncio.Semaphore(max_inflight)

        self.inflight = 0
        self.success = 0
        self.errors = 0

        self.queue_wait_ema = None
        self.latency_ema = None
        self.alpha = 0.2

        self.start_ts = time.time()

    async def _acquire_slot(self):
        while self.inflight >= self.current_limit:
            await asyncio.sleep(0.001)
        await self.sem.acquire()
        self.inflight += 1

    def _release_slot(self):
        self.inflight -= 1
        self.sem.release()

    def _update_latency(self, latency):
        if self.latency_ema is None:
            self.latency_ema = latency
        else:
            self.latency_ema = self.alpha * latency + (1 - self.alpha) * self.latency_ema

    def _update_queue_wait(self, wait_ms):
        if self.queue_wait_ema is None:
            self.queue_wait_ema = wait_ms
        else:
            self.queue_wait_ema = self.alpha * wait_ms + (1 - self.alpha) * self.queue_wait_ema

    def telemetry(self):
        elapsed = max(time.time() - self.start_ts, 1)
        return {
            "window": self.current_limit,
            "inflight": self.inflight,
            "latency_ema_ms": round(self.latency_ema or 0, 2),
            "queue_wait_ema_ms": round(self.queue_wait_ema or 0, 2),
            "success": self.success,
            "errors": self.errors,
            "rps": round((self.success + self.errors) / elapsed, 2),
            "client": self.client.telemetry() if hasattr(self, "client") else None,
        }