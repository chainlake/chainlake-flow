import asyncio
import time
from abc import ABC, abstractmethod
from opentelemetry import trace, metrics
from opentelemetry.trace import Status, StatusCode

tracer = trace.get_tracer("rpcstream.client")

from rpcstream.metrics.client import (
    REQUEST_COUNTER,
    REQUEST_SUBMITTED_COUNTER,
    INFLIGHT_GAUGE,
    RETRY_COUNTER,
    LATENCY_HISTOGRAM,
)

class BaseClient(ABC):
    """
    Base transport client with shared retry / metrics / tracing logic.
    """

    def __init__(self, base_url: str, max_retries: int = 2, logger=None):
        self.base_url = base_url.rstrip("/")
        self.max_retries = max_retries
        self.logger = logger
        
    async def execute(self, request):
        start = time.time()
        method = request.method
        
        # OTEL metrics
        REQUEST_SUBMITTED_COUNTER.add(1, {"method": method})
        INFLIGHT_GAUGE.add(1)
        
        if self.logger and self.logger.isEnabledFor(10):
            self.logger.debug(
                "rpc.request",
                component="client",
                rpc_url=self.base_url,
                method=method,
                params_preview=str(request.params)[:100]
            )

        with tracer.start_as_current_span("rpc.execute") as span:
            span.set_attribute("rpc.url", self.base_url)
            span_ctx = span.get_span_context()
            trace_id = format(span_ctx.trace_id, "032x")

            try:
                for attempt in range(self.max_retries + 1):
                    try:
                        result = await self._execute(request, span)
                        REQUEST_COUNTER.add(1, {"method": method, "status": "success"})
                        
                        span.set_status(Status(StatusCode.OK))
                        span.set_attribute("rpc.method", method)
                        span.set_attribute("rpc.retry_count", attempt)
                        span.set_attribute("rpc.system", "jsonrpc")
                        
                        if self.logger and self.logger.isEnabledFor(10):
                            self.logger.debug(
                                "rpc.success",
                                component="client",
                                method=method,
                                attempt=attempt,
                                trace_id=trace_id,
                            )
                        
                        return result

                    except asyncio.TimeoutError:
                        REQUEST_COUNTER.add(1, {"method": method, "status": "timeout"})
                        span.set_attribute("rpc.status", "timeout")
                        span.add_event("retry", {"attempt": attempt})
                        
                        if self.logger:
                            self.logger.warn(
                                "rpc.timeout",
                                component="client",
                                method=method,
                                attempt=attempt,
                                trace_id=trace_id,
                            )

                        if attempt >= self.max_retries:
                            raise

                        RETRY_COUNTER.add(1, {"method": method})
                        await asyncio.sleep(0.1 * (attempt + 1))

                    except Exception as exc:
                        REQUEST_COUNTER.add(1, {"method": method, "status": "transport_error"})
                        span.add_event("retry", {"attempt": attempt})
                        span.set_status(Status(StatusCode.ERROR))

                        if self.logger:
                            self.logger.warn(
                                "rpc.transport_error",
                                component="client",
                                method=method,
                                attempt=attempt,
                                error=str(exc),
                                trace_id=trace_id,
                            )

                        if attempt >= self.max_retries:
                            raise

                        RETRY_COUNTER.add(1, {"method": method})
                        await asyncio.sleep(0.1 * (attempt + 1))

            except Exception as exc:
                error_msg = repr(exc)
                
                REQUEST_COUNTER.add(1, {"method": method, "status": "request_error"})
                span.set_attribute("rpc.status", "failed")
                span.set_attribute("rpc.exception", error_msg)
                
                if self.logger:
                    self.logger.error(
                        "rpc.failed",
                        component="client",
                        method=method,
                        error=error_msg,
                        trace_id=trace_id,
                    )
                
                raise

            finally:
                latency = (time.time() - start) * 1000
                
                # OTEL (authoritative)
                LATENCY_HISTOGRAM.record(latency, {"method": method})
                INFLIGHT_GAUGE.add(-1)
                
                span.set_attribute("rpc.latency_ms", round(latency, 2))
                
                if self.logger:
                    self.logger.debug(
                        "rpc.complete",
                        component="client",
                        method=method,
                        latency_ms=round(latency, 2),
                        trace_id=trace_id,
                    )

    @abstractmethod
    async def _execute(self, request, span):
        """
        Transport-specific execution.
        """
        pass