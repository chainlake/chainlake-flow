import asyncio
import time
import uuid
import aiohttp
import orjson
from typing import Any
# OpenTelemetry
from opentelemetry import trace
from opentelemetry.trace import Tracer
tracer = trace.get_tracer("rpcstream.rpc")

from rpcstream.rpc.models import RpcTaskMeta, RpcErrorResult, ClientMetrics

# -------------------------
# Client with trace hook
# -------------------------
class RpcClient:
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