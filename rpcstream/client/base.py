import asyncio
import time
from abc import ABC, abstractmethod
from opentelemetry.trace import Status, StatusCode
from contextlib import nullcontext

from rpcstream.metrics.client import ClientMetrics
from rpcstream.client.models import exception_log_fields, is_expected_rpc_warning, summarize_exception
from rpcstream.runtime.observability.context import ObservabilityContext

class BaseClient(ABC):
    """
    Base transport client with shared retry / metrics / tracing logic.
    """

    def __init__(
        self,
        base_url: str,
        max_retries: int = 2,
        logger=None,
        observability: ObservabilityContext | None = None,
    ):
        self.base_url = base_url.rstrip("/")
        self.max_retries = max_retries
        self.logger = logger
        self.observability = observability or ObservabilityContext.disabled()
        self._tracer = self.observability.get_tracer(__name__)
        self._metrics = ClientMetrics(self.observability.get_meter("rpcstream.client"))
        
    @property
    def metrics(self):
        return self._metrics
        
    async def execute(self, request, trace_request: bool = True):
        start = time.time()
        method = request.method
        
        # OTEL metrics
        self.metrics.REQUEST_SUBMITTED_COUNTER.add(1, {"method": method})
        self.metrics.INFLIGHT_GAUGE.add(1)
        
        if self.logger:
            self.logger.debug(
                "rpc.request",
                component="client",
                rpc_url=self.base_url,
                method=method,
                params_preview=str(request.params)[:100]
            )

        span_context = (
            self._tracer.start_as_current_span("rpc.execute")
            if trace_request
            else nullcontext()
        )

        with span_context as span:
            if span is None:
                span = None
            if span is not None:
                span.set_attribute("rpc.url", self.base_url)

            try:
                for attempt in range(self.max_retries + 1):
                    try:
                        result = await self._execute(request, span)
                        self.metrics.REQUEST_COUNTER.add(1, {"method": method, "status": "success"})
                        
                        if span is not None:
                            span.set_status(Status(StatusCode.OK))
                            span.set_attribute("rpc.method", method)
                            span.set_attribute("rpc.retry_count", attempt)
                            span.set_attribute("rpc.system", "jsonrpc")
                        
                        if self.logger:
                            self.logger.debug(
                                "rpc.success",
                                component="client",
                                method=method,
                                attempt=attempt,
                            )
                        
                        return result

                    except asyncio.TimeoutError:
                        self.metrics.REQUEST_COUNTER.add(1, {"method": method, "status": "timeout"})
                        if span is not None:
                            span.set_attribute("rpc.status", "timeout")
                            span.add_event("retry", {"attempt": attempt})
                        
                        if self.logger:
                            self.logger.warn(
                                "rpc.timeout",
                                component="client",
                                method=method,
                                attempt=attempt,
                            )

                        if attempt >= self.max_retries:
                            raise

                        self.metrics.RETRY_COUNTER.add(1, {"method": method})
                        await asyncio.sleep(0.1 * (attempt + 1))

                    except Exception as exc:
                        self.metrics.REQUEST_COUNTER.add(1, {"method": method, "status": "transport_error"})
                        if span is not None:
                            span.add_event("retry", {"attempt": attempt})
                            span.set_status(Status(StatusCode.ERROR))

                        if self.logger:
                            self.logger.warn(
                                "rpc.transport_error",
                                component="client",
                                method=method,
                                attempt=attempt,
                                **exception_log_fields(exc),
                            )

                        if attempt >= self.max_retries:
                            raise

                        self.metrics.RETRY_COUNTER.add(1, {"method": method})
                        await asyncio.sleep(0.1 * (attempt + 1))

            except Exception as exc:
                error_msg = summarize_exception(exc)
                
                self.metrics.REQUEST_COUNTER.add(1, {"method": method, "status": "request_error"})
                if span is not None:
                    span.set_attribute("rpc.status", "failed")
                    span.set_attribute("rpc.exception", error_msg)
                
                if self.logger:
                    log_method = self.logger.warn if is_expected_rpc_warning(exc) else self.logger.error
                    log_method(
                        "rpc.failed",
                        component="client",
                        method=method,
                        **exception_log_fields(exc),
                    )
                
                raise

            finally:
                latency = (time.time() - start) * 1000
                
                # OTEL (authoritative)
                self.metrics.LATENCY_HISTOGRAM.record(latency, {"method": method})
                self.metrics.INFLIGHT_GAUGE.add(-1)
                
                if span is not None:
                    span.set_attribute("rpc.latency_ms", round(latency, 2))
                
                if self.logger:
                    self.logger.debug(
                        "rpc.complete",
                        component="client",
                        method=method,
                        latency_ms=round(latency, 2),
                    )

    @abstractmethod
    async def _execute(self, request, span):
        """
        Transport-specific execution.
        """
        pass
