import asyncio
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Optional

import aiohttp
import orjson

# OpenTelemetry
from opentelemetry import trace
from opentelemetry.trace import Tracer

tracer = trace.get_tracer(__name__)

@dataclass
class RpcTaskMeta:
    task_id: int
    submit_ts: float
    extra: dict[str, Any]

@dataclass
class RpcErrorResult:
    error: Exception
    meta: RpcTaskMeta

@dataclass
class ClientMetrics:
    request_total: int = 0
    request_success: int = 0
    request_error: int = 0
    retry_total: int = 0
    timeout_total: int = 0
    transport_error_total: int = 0
    rpc_error_total: int = 0
    inflight: int = 0
    latency_ema_ms: Optional[float] = None

# -------------------------
# Client with trace hook
# -------------------------
class ErpcClient:
    """Thin eRPC client with session reuse, connector tuning, trace and metrics.
    
    Retry/circuit-breaker/failover/routing are delegated to eRPC gateway. 
    """
    def __init__(
        self,
        base_url: str,
        timeout_sec: int = 10,
        pool_limit: int = 200,
        dns_ttl_sec: int = 300,
        max_retries: int = 3,
    ):
        self.base_url = base_url.rstrip("/")
        self.max_retries = max_retries
        self.metrics = ClientMetrics()

        timeout = aiohttp.ClientTimeout(total=timeout_sec)
        connector = aiohttp.TCPConnector(
            limit=pool_limit,
            ttl_dns_cache=dns_ttl_sec,
            keepalive_timeout=30,
            enable_cleanup_closed=True,
        )

        self.session = aiohttp.ClientSession(
            timeout=timeout,
            connector=connector,
            json_serialize=lambda obj: orjson.dumps(obj).decode(),
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
                "Accept-Encoding": "gzip, deflate",
            },
        )

    async def call(self, method: str, params: list[Any]) -> Any:
        start = time.time()
        payload = {"jsonrpc": "2.0", "id": str(uuid.uuid4()), "method": method, "params": params}

        self.metrics.request_total += 1
        self.metrics.inflight += 1

        # -------------------------
        # Client Trace Span
        # -------------------------
        with tracer.start_as_current_span("erpc.call") as span:
            span.set_attribute("rpc.method", method)
            span.set_attribute("rpc.url", self.base_url)

            try:
                for attempt in range(self.max_retries + 1):
                    try:
                        async with self.session.post(self.base_url, json=payload) as resp:
                            resp.raise_for_status()
                            raw = await resp.read()
                            data = orjson.loads(raw)

                        if "error" in data:
                            self.metrics.rpc_error_total += 1
                            span.set_attribute("rpc.status", "error")
                            span.set_attribute("rpc.error", str(data["error"]))
                            raise RuntimeError(data["error"])

                        self.metrics.request_success += 1
                        span.set_attribute("rpc.status", "ok")
                        return data["result"]

                    except asyncio.TimeoutError:
                        self.metrics.timeout_total += 1
                        span.set_attribute("rpc.status", "timeout")
                        if attempt >= self.max_retries:
                            raise
                        self.metrics.retry_total += 1
                        await asyncio.sleep(0.1 * (attempt + 1))

                    except aiohttp.ClientError as exc:
                        self.metrics.transport_error_total += 1
                        span.set_attribute("rpc.status", "transport_error")
                        span.set_attribute("rpc.error", str(exc))
                        if attempt >= self.max_retries:
                            raise
                        self.metrics.retry_total += 1
                        await asyncio.sleep(0.1 * (attempt + 1))

            except Exception as exc:
                self.metrics.request_error += 1
                span.set_attribute("rpc.status", "failed")
                span.set_attribute("rpc.exception", str(exc))
                raise

            finally:
                latency = (time.time() - start) * 1000
                if self.metrics.latency_ema_ms is None:
                    self.metrics.latency_ema_ms = latency
                else:
                    self.metrics.latency_ema_ms = 0.2 * latency + 0.8 * self.metrics.latency_ema_ms
                self.metrics.inflight -= 1
                span.set_attribute("rpc.latency_ms", round(latency, 2))

    async def close(self):
        await self.session.close()

    def telemetry(self):
        return vars(self.metrics)


class AdaptiveErpcScheduler:
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
        client: ErpcClient,
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