from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.metrics.export import InMemoryMetricReader

from rpcstream.runtime.observability.context import ObservabilityContext


def build_observability(config, service_name: str) -> ObservabilityContext:
    resource = Resource.create({"service.name": service_name})

    tracer_provider = None
    meter_provider = None
    logger_provider = None
    metric_reader = None
    trace_exporter = None
    metric_exporter = None
    log_exporter = None

    # ---------------- TRACING ----------------
    if config.tracing.enabled:
        if not config.tracing.endpoint:
            raise ValueError("telemetry.tracing.endpoint is required when tracing is enabled")

        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.sampling import ParentBased, TraceIdRatioBased
        from opentelemetry.sdk.trace.export import SimpleSpanProcessor
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

        tracer_provider = TracerProvider(
            resource=resource,
            sampler=ParentBased(TraceIdRatioBased(config.tracing.sampleRate)),
        )

        trace_exporter = OTLPSpanExporter(
            endpoint=config.tracing.endpoint,
            insecure=True,
            timeout=5,
        )

        tracer_provider.add_span_processor(SimpleSpanProcessor(trace_exporter))

    # ---------------- METRICS ----------------
    if config.metrics.enabled:
        if not config.metrics.endpoint:
            raise ValueError("telemetry.metrics.endpoint is required when metrics is enabled")

        from opentelemetry.sdk.metrics import MeterProvider
        from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter

        metric_exporter = OTLPMetricExporter(
            endpoint=config.metrics.endpoint,
            insecure=True,
            timeout=5,
        )

        metric_reader = InMemoryMetricReader()

        meter_provider = MeterProvider(
            resource=resource,
            metric_readers=[metric_reader],
        )

    # ---------------- LOGS ----------------
    if config.logs.enabled:
        if not config.logs.endpoint:
            raise ValueError("telemetry.logs.endpoint is required when logs is enabled")

        from opentelemetry.sdk._logs import LoggerProvider
        from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
        from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter

        logger_provider = LoggerProvider(resource=resource)
        log_exporter = OTLPLogExporter(
            endpoint=config.logs.endpoint,
            insecure=True,
            timeout=5,
        )
        logger_provider.add_log_record_processor(
            BatchLogRecordProcessor(log_exporter)
        )

    return ObservabilityContext(
        service_name=service_name,
        tracing_enabled=config.tracing.enabled,
        metrics_enabled=config.metrics.enabled,
        logs_enabled=config.logs.enabled,
        tracer_provider=tracer_provider,
        meter_provider=meter_provider,
        logger_provider=logger_provider,
        metric_reader=metric_reader,
        trace_exporter=trace_exporter,
        metric_exporter=metric_exporter,
        log_exporter=log_exporter,
        metrics_export_interval_ms=config.metrics.export_interval_ms,
    )
