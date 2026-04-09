import asyncio
import time
from abc import ABC, abstractmethod

from opentelemetry import trace
from rpcstream.client.models import ClientMetrics

tracer = trace.get_tracer("rpcstream.client")


class BaseClient(ABC):
    """
    Base transport client with shared retry / metrics / tracing logic.
    """

    def __init__(self, base_url: str, max_retries: int = 2):
        self.base_url = base_url.rstrip("/")
        self.max_retries = max_retries
        self.metrics = ClientMetrics()

    async def execute(self, request):
        start = time.time()
        self.metrics.request_total += 1
        self.metrics.inflight += 1

        with tracer.start_as_current_span("rpc.execute") as span:
            span.set_attribute("rpc.url", self.base_url)

            try:
                for attempt in range(self.max_retries + 1):
                    try:
                        result = await self._execute(request, span)
                        self.metrics.request_success += 1
                        span.set_attribute("rpc.status", "ok")
                        return result

                    except asyncio.TimeoutError:
                        self.metrics.timeout_total += 1
                        span.set_attribute("rpc.status", "timeout")

                        if attempt >= self.max_retries:
                            raise

                        self.metrics.retry_total += 1
                        await asyncio.sleep(0.1 * (attempt + 1))

                    except Exception as exc:
                        self.metrics.transport_error_total += 1
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

    @abstractmethod
    async def _execute(self, request, span):
        """
        Transport-specific execution.
        """
        pass

    def telemetry(self):
        return vars(self.metrics)