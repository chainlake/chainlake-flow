"""Tests for SchedulerMetrics — the observable gauges that surface the adaptive
latency controller state to OTel/Grafana.

Covers:
- NoOp mode (meter=None): all attributes present, callbacks yield nothing
  when unbound and yield Observations when bound to a scheduler.
- Real OTel SDK mode: all 4 new metrics register as observable gauges with
  the expected names, units, and current values pulled from _update_latency
  / _update_floor / effective_target_ms.
"""
import pytest
from opentelemetry.sdk.metrics import MeterProvider as SDKMeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader

from rpcstream.metrics.scheduler import SchedulerMetrics
from rpcstream.scheduler.base import BaseScheduler


def _feed(scheduler, latencies):
    """Drive the controller through _update_latency / _update_floor."""
    for latency in latencies:
        scheduler._update_latency(latency)


def _make_scheduler():
    return BaseScheduler(
        min_inflight=1,
        max_inflight=20,
        initial_inflight=5,
        latency_target_ms=0,
        target_multiplier=3.0,
    )


# ----- NoOp mode ---------------------------------------------------------


def test_noop_mode_exposes_all_seven_attributes():
    metrics = SchedulerMetrics()  # meter=None
    for name in (
        "STATE",
        "FAILURE_RATE",
        "CONSECUTIVE",
        "LATENCY_FLOOR_MS",
        "LATENCY_EMA_MS",
        "EFFECTIVE_TARGET_MS",
        "CURRENT_LIMIT",
    ):
        assert hasattr(metrics, name), f"missing NoOp placeholder: {name}"


def test_callbacks_yield_nothing_when_unbound():
    metrics = SchedulerMetrics()
    for cb in (
        metrics._observe_latency_floor,
        metrics._observe_latency_ema,
        metrics._observe_effective_target,
        metrics._observe_current_limit,
    ):
        assert list(cb(None)) == []


def test_callbacks_yield_observations_when_bound():
    sched = _make_scheduler()
    _feed(sched, [100, 110, 105, 95, 90, 88, 92])

    metrics = SchedulerMetrics()
    metrics.bind(sched)

    floor = list(metrics._observe_latency_floor(None))[0].value
    ema = list(metrics._observe_latency_ema(None))[0].value
    target = list(metrics._observe_effective_target(None))[0].value
    limit = list(metrics._observe_current_limit(None))[0].value

    assert floor == pytest.approx(sched.latency_floor)
    assert ema == pytest.approx(sched.latency_ema)
    assert target == pytest.approx(sched.effective_target_ms())
    assert limit == pytest.approx(sched.current_limit)

    # Invariant: effective_target = floor * multiplier (when no hard floor).
    assert target == pytest.approx(floor * sched.target_multiplier)


def test_callbacks_report_zero_before_any_update():
    """Cold start: latency_floor / latency_ema are None; gauges must report
    0 (not raise) so the scrape never fails on a fresh process."""
    sched = _make_scheduler()
    metrics = SchedulerMetrics()
    metrics.bind(sched)

    floor = list(metrics._observe_latency_floor(None))[0].value
    ema = list(metrics._observe_latency_ema(None))[0].value
    target = list(metrics._observe_effective_target(None))[0].value
    limit = list(metrics._observe_current_limit(None))[0].value

    assert floor == 0.0
    assert ema == 0.0
    assert target == 0.0  # floor * 3 = 0 because floor is None/0
    assert limit == pytest.approx(sched.current_limit)


# ----- Real OTel SDK mode ------------------------------------------------


EXPECTED_NEW_METRICS = {
    "rpcstream_scheduler_latency_floor_ms",
    "rpcstream_scheduler_latency_ema_ms",
    "rpcstream_scheduler_effective_target_ms",
    "rpcstream_scheduler_current_limit",
}


def _make_real_metrics(scheduler):
    reader = InMemoryMetricReader()
    provider = SDKMeterProvider(metric_readers=[reader])
    meter = provider.get_meter("rpcstream.scheduler")
    metrics = SchedulerMetrics(meter=meter)
    metrics.bind(scheduler)
    return reader, provider


def _scrape(reader):
    reader.collect()
    data = reader.get_metrics_data()
    by_name = {}
    for rm in data.resource_metrics:
        for sm in rm.scope_metrics:
            for m_obj in sm.metrics:
                dp = next(iter(m_obj.data.data_points), None)
                if dp is not None and hasattr(dp, "value"):
                    by_name[m_obj.name] = dp.value
    return by_name


def test_real_otel_registers_all_new_gauges():
    sched = _make_scheduler()
    reader, _ = _make_real_metrics(sched)
    scraped = _scrape(reader)
    missing = EXPECTED_NEW_METRICS - scraped.keys()
    assert not missing, f"missing metrics: {missing}"


def test_real_otel_reports_post_update_latency_values():
    sched = _make_scheduler()
    _feed(sched, [100, 110, 105, 95, 90, 88, 92])

    reader, _ = _make_real_metrics(sched)
    scraped = _scrape(reader)

    # Floor should converge toward the lower bound of the input range.
    assert 80 < scraped["rpcstream_scheduler_latency_floor_ms"] < 120
    # EMA is similar.
    assert 80 < scraped["rpcstream_scheduler_latency_ema_ms"] < 120
    # Target = floor * 3.
    floor = scraped["rpcstream_scheduler_latency_floor_ms"]
    target = scraped["rpcstream_scheduler_effective_target_ms"]
    assert target == pytest.approx(floor * 3.0)
    # current_limit reflects initial_inflight; no _adjust_window was called.
    assert scraped["rpcstream_scheduler_current_limit"] == pytest.approx(5.0)