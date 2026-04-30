import asyncio
from pathlib import Path

import pytest

from rpcstream.config.loader import load_pipeline_config
from rpcstream.config.resolver import resolve
from rpcstream.runtime.observability.context import ObservabilityContext
from rpcstream.runtime.observability.provider import build_observability


class FakeMetricReader:
    def __init__(self):
        self.collect_calls = 0

    def collect(self):
        self.collect_calls += 1

    def get_metrics_data(self):
        return {"collect_calls": self.collect_calls}


class FakeMetricExporter:
    def __init__(self):
        self.exports = []
        self.shutdown_calls = 0

    def export(self, metrics_data, timeout_millis=10000, **kwargs):
        self.exports.append((metrics_data, timeout_millis))

    def shutdown(self, timeout_millis=30000, **kwargs):
        self.shutdown_calls += 1


class FakeTracerProvider:
    def __init__(self):
        from opentelemetry.trace import NoOpTracerProvider

        self._provider = NoOpTracerProvider()
        self.flush_calls = 0
        self.shutdown_calls = 0

    def get_tracer(self, name):
        return self._provider.get_tracer(name)

    def force_flush(self, timeout_millis=30000):
        self.flush_calls += 1
        return True

    def shutdown(self):
        self.shutdown_calls += 1


class FakeTraceExporter:
    def __init__(self):
        self.shutdown_calls = 0

    def shutdown(self, timeout_millis=30000, **kwargs):
        self.shutdown_calls += 1


def test_observability_context_exports_with_explicit_async_lifecycle():
    metric_reader = FakeMetricReader()
    metric_exporter = FakeMetricExporter()
    tracer_provider = FakeTracerProvider()
    trace_exporter = FakeTraceExporter()

    context = ObservabilityContext(
        service_name="test-service",
        metrics_enabled=True,
        tracing_enabled=True,
        meter_provider=None,
        tracer_provider=tracer_provider,
        metric_reader=metric_reader,
        metric_exporter=metric_exporter,
        trace_exporter=trace_exporter,
        metrics_export_interval_ms=10,
    )

    async def run():
        await context.start()
        await asyncio.sleep(0.03)
        await context.shutdown()

    asyncio.run(run())

    assert metric_reader.collect_calls >= 1
    assert metric_exporter.exports
    assert metric_exporter.shutdown_calls == 1
    assert tracer_provider.flush_calls >= 1
    assert tracer_provider.shutdown_calls == 1
    assert trace_exporter.shutdown_calls == 1


def test_pipeline_telemetry_alias_maps_into_runtime_observability():
    config_path = Path(__file__).resolve().parents[2] / "pipeline.yaml"

    config = load_pipeline_config(str(config_path))
    config.observability.metrics.enabled = True
    config.observability.tracing.enabled = True
    config.observability.tracing.sampleRate = 0.01
    config.observability.logs.enabled = True
    runtime = resolve(config)

    assert runtime.observability.config.metrics.enabled is True
    assert runtime.observability.config.tracing.enabled is True
    assert runtime.observability.config.tracing.sampleRate == 0.01
    assert runtime.observability.config.metrics.endpoint == "http://localhost:4317"
    assert runtime.observability.config.metrics.export_interval_ms == 5000
    assert runtime.observability.config.logs.enabled is True
    assert runtime.observability.config.logs.endpoint == "http://localhost:4317"


def test_build_observability_requires_endpoint_when_enabled():
    config_path = Path(__file__).resolve().parents[2] / "pipeline.yaml"
    config = load_pipeline_config(str(config_path))

    config.observability.metrics.enabled = True
    config.observability.metrics.endpoint = None

    with pytest.raises(ValueError, match="telemetry.metrics.endpoint"):
        build_observability(config.observability, "test-service")


def test_build_observability_uses_tracing_sample_rate():
    config_path = Path(__file__).resolve().parents[2] / "pipeline.yaml"
    config = load_pipeline_config(str(config_path))

    config.observability.tracing.enabled = True
    config.observability.tracing.endpoint = "http://localhost:4317"
    config.observability.tracing.sampleRate = 0.25

    context = build_observability(config.observability, "test-service")
    root_sampler = context._tracer_provider.sampler._root

    assert root_sampler.rate == 0.25
