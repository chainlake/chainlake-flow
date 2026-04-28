import asyncio

from opentelemetry.metrics import NoOpMeterProvider
from opentelemetry.trace import NoOpTracerProvider


class ObservabilityContext:
    """
    Deterministic runtime observability container.
    Providers, exporters, and export scheduling are owned explicitly here.
    """

    def __init__(
        self,
        *,
        service_name: str,
        tracing_enabled: bool = False,
        metrics_enabled: bool = False,
        logs_enabled: bool = False,
        tracer_provider=None,
        meter_provider=None,
        logger_provider=None,
        metric_reader=None,
        trace_exporter=None,
        metric_exporter=None,
        log_exporter=None,
        metrics_export_interval_ms: int = 5000,
    ):
        self.service_name = service_name
        self.tracing_enabled = tracing_enabled
        self.metrics_enabled = metrics_enabled
        self.logs_enabled = logs_enabled
        self._tracer_provider = tracer_provider or NoOpTracerProvider()
        self._meter_provider = meter_provider or NoOpMeterProvider()
        self._logger_provider = logger_provider
        self._metric_reader = metric_reader
        self._trace_exporter = trace_exporter
        self._metric_exporter = metric_exporter
        self._log_exporter = log_exporter
        self._metrics_export_interval_ms = metrics_export_interval_ms
        self._metrics_task = None
        self._stop_event = asyncio.Event()

    @classmethod
    def disabled(cls, service_name: str = "rpcstream"):
        return cls(service_name=service_name)

    def get_tracer(self, name: str):
        return self._tracer_provider.get_tracer(name)

    def get_meter(self, name: str):
        return self._meter_provider.get_meter(name)

    def get_logger_provider(self):
        return self._logger_provider

    async def start(self):
        if not self.metrics_enabled or self._metric_reader is None or self._metric_exporter is None:
            return
        if self._metrics_task is not None:
            return

        self._stop_event.clear()
        self._metrics_task = asyncio.create_task(self._run_metrics_loop())

    async def _run_metrics_loop(self):
        interval_sec = max(self._metrics_export_interval_ms, 1) / 1000.0

        while not self._stop_event.is_set():
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=interval_sec)
            except asyncio.TimeoutError:
                self.export_metrics()

    def export_metrics(self):
        if not self.metrics_enabled or self._metric_reader is None or self._metric_exporter is None:
            return

        self._metric_reader.collect()
        metrics_data = self._metric_reader.get_metrics_data()
        if metrics_data is None:
            return

        self._metric_exporter.export(metrics_data, timeout_millis=10000)

    def flush_traces(self):
        if (
            not self.tracing_enabled
            or self._tracer_provider is None
            or not hasattr(self._tracer_provider, "force_flush")
        ):
            return

        self._tracer_provider.force_flush(timeout_millis=10000)

    async def shutdown(self):
        self._stop_event.set()

        if self._metrics_task is not None:
            await self._metrics_task
            self._metrics_task = None

        self.export_metrics()
        self.flush_traces()

        if self._metric_exporter is not None:
            self._metric_exporter.shutdown(timeout_millis=10000)

        if self._trace_exporter is not None:
            self._trace_exporter.shutdown(timeout_millis=10000)

        if (
            self._logger_provider is not None
            and self.logs_enabled
            and hasattr(self._logger_provider, "shutdown")
        ):
            self._logger_provider.shutdown()

        if (
            self._meter_provider is not None
            and self.metrics_enabled
            and hasattr(self._meter_provider, "shutdown")
        ):
            self._meter_provider.shutdown(timeout_millis=10000)

        if (
            self._tracer_provider is not None
            and self.tracing_enabled
            and hasattr(self._tracer_provider, "shutdown")
        ):
            self._tracer_provider.shutdown()
