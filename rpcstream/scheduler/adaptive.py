
from rpcstream.rpc.rpc_client import RpcClient
from rpcstream.rpc.models import RpcTaskMeta, RpcErrorResult

class AdaptiveRpcScheduler:
    """
    Adaptive eRPC Scheduler with smooth concurrency adjustment based on observed latency and task success.

    Features:
    - Dynamically adjusts concurrency (current_limit) to hit target latency.
    - Smooth growth if latency is below target.
    - Gradual or fast reduction if latency exceeds target or task fails.
    - Queue wait EMA tracking for monitoring.
    """
    def __init__(
        self,
        client: RpcClient,
        min_inflight=5,
        max_inflight=50,
        initial_inflight=10,
        latency_target_ms=200,
    ):
        self.client = client
        self.min_inflight = min_inflight
        self.max_inflight = max_inflight
        self.current_limit = initial_inflight
        self.latency_target_ms = latency_target_ms

        self.sem = asyncio.Semaphore(initial_inflight)
        self.inflight = 0
        self.success = 0
        self.errors = 0

        # Exponential moving averages
        self.queue_wait_ema = None
        self.latency_ema = None
        self.alpha = 0.2
        self.start_ts = time.time()

    async def submit(self, method, params, meta_extra):
        """
        Submit a task to the scheduler.
        Measures queue wait time and latency.
        Adjusts concurrency based on latency EMA.
        """
        enqueue_ts = time.time()
        with tracer.start_as_current_span("scheduler.submit") as span:
            span.set_attribute("rpc.method", method)

            # Acquire semaphore to respect current concurrency limit
            await self.sem.acquire()
            wait_ms = (time.time() - enqueue_ts) * 1000
            self._update_queue_wait(wait_ms)
            self.inflight += 1

            submit_ts = time.time()
            meta = RpcTaskMeta(
                task_id=id(asyncio.current_task()),
                submit_ts=submit_ts,
                extra=meta_extra
            )
            meta.extra["queue_wait_ms"] = wait_ms
            span.set_attribute("scheduler.queue_wait_ms", round(wait_ms, 2))
            span.set_attribute("scheduler.window", self.current_limit)

            try:
                result = await self.client.call(method, params)
                latency = (time.time() - submit_ts) * 1000
                self.success += 1
                self._update_latency(latency)
                self._adjust_window(True)
                meta.extra["latency_ms"] = round(latency, 2)

                span.set_attribute("scheduler.status", "ok")
                span.set_attribute("scheduler.latency_ms", round(latency, 2))
                return result, meta

            except Exception as exc:
                latency = (time.time() - submit_ts) * 1000
                self.errors += 1
                self._update_latency(latency)
                self._adjust_window(False)

                span.set_attribute("scheduler.status", "error")
                span.set_attribute("scheduler.exception", str(exc))
                span.set_attribute("scheduler.latency_ms", round(latency, 2))
                return RpcErrorResult(exc, meta)

            finally:
                self.inflight -= 1
                self.sem.release()

    def _update_latency(self, latency):
        """Update the exponential moving average of latency"""
        if self.latency_ema is None:
            self.latency_ema = latency
        else:
            self.latency_ema = self.alpha * latency + (1 - self.alpha) * self.latency_ema

    def _update_queue_wait(self, wait_ms):
        """Update the exponential moving average of queue wait time"""
        if self.queue_wait_ema is None:
            self.queue_wait_ema = wait_ms
        else:
            self.queue_wait_ema = self.alpha * wait_ms + (1 - self.alpha) * self.queue_wait_ema

    def _adjust_window(self, success):
        """
        Adjust the concurrency window based on latency EMA and success/failure.
        - Success and low latency: slightly increase concurrency
        - High latency: gradually decrease concurrency
        - Extreme latency or failure: fast decrease concurrency
        """
        cur = self.current_limit
        min_limit = self.min_inflight
        max_limit = self.max_inflight

        increase_step = 1           # step to increase concurrency
        mild_decrease_factor = 0.95 # gentle reduction
        strong_decrease_factor = 0.85 # strong reduction for failure or extreme latency

        if not success:
            # Task failed: strong reduction
            self.current_limit = max(min_limit, int(cur * strong_decrease_factor))
            return

        latency = self.latency_ema or self.latency_target_ms

        if latency > self.latency_target_ms * 3:
            # Extreme latency: strong reduction
            self.current_limit = max(min_limit, int(cur * strong_decrease_factor))
        elif latency > self.latency_target_ms:
            # Slightly high latency: gentle reduction
            self.current_limit = max(min_limit, int(cur * mild_decrease_factor))
        else:
            # Low latency: gentle increase
            self.current_limit = min(max_limit, cur + increase_step)

    def telemetry(self):
        """Return real-time metrics of the scheduler"""
        elapsed = max(time.time() - self.start_ts, 1)
        return {
            "window": self.current_limit,
            "inflight": self.inflight,
            "latency_ema_ms": round(self.latency_ema or 0, 2),
            "queue_wait_ema_ms": round(self.queue_wait_ema or 0, 2),
            "success": self.success,
            "errors": self.errors,
            "rps": round((self.success + self.errors) / elapsed, 2),
        }