from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter


def init_telemetry(service_name: str = "rpcstream"):
    # ---- tracing ----
    resource = Resource.create({
        "service.name": service_name
    })

    provider = TracerProvider(resource=resource)
    trace.set_tracer_provider(provider)

    otlp_exporter = OTLPSpanExporter(
        endpoint="http://otel-collector.observability.svc:4317",  # or localhost
        insecure=True,
        timeout=5,
    )

    span_processor = BatchSpanProcessor(otlp_exporter)
    provider.add_span_processor(span_processor)
    
    # ---- metrics ----
    metric_exporter = OTLPMetricExporter(
        endpoint="http://otel-collector.observability.svc:4317",
        insecure=True,
        timeout=5,
    )

    reader = PeriodicExportingMetricReader(metric_exporter, export_interval_millis=5000)

    meter_provider = MeterProvider(
        resource=Resource.create({"service.name": service_name}),
        metric_readers=[reader],
    )

    from opentelemetry import metrics
    metrics.set_meter_provider(meter_provider)