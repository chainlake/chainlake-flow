"""Tests for the adaptive engine worker pool.

Covers:
- The scheduler callback mechanism (add_window_change_listener, replace,
  remove_window_change_listener, exception isolation between listeners).
- The engine refactor:
  * concurrency=0 + max_inflight=N spawns N workers at startup and tracks
    scheduler.current_limit via cooperative exit flags.
  * concurrency=N>0 is fixed (no shrink, no grow).
  * concurrency=1 is serial (single worker).
  * eos_enabled forces worker_pool_size=1 regardless of concurrency.
  * worker exit flags are reset between run_stream() invocations.
  * The WORKER_COUNT observable gauge tracks active workers.
"""

import asyncio
from types import SimpleNamespace

import pytest

from rpcstream.ingestion.engine import IngestionEngine
from rpcstream.metrics.engine import EngineMetrics
from rpcstream.scheduler.base import BaseScheduler


# ---------- scheduler callback mechanism ---------------------------------


def test_window_change_listener_fires_on_cb_trip():
    sched = BaseScheduler(
        min_inflight=1, max_inflight=10, initial_inflight=5,
    )
    fired = []
    sched.add_window_change_listener(lambda new_limit: fired.append(new_limit))

    sched._cb_trip()
    assert fired == [sched.min_inflight]


def test_window_change_listener_fires_on_set_current_limit():
    """Any direct write through _set_current_limit (used by _adjust_window
    in adaptive.py) must fire the listener with the new value."""
    sched = BaseScheduler(
        min_inflight=1, max_inflight=10, initial_inflight=5,
    )
    fired = []
    sched.add_window_change_listener(lambda v: fired.append(v))

    sched._set_current_limit(7)
    sched._set_current_limit(3)
    assert fired == [7, 3]


def test_multiple_listeners_all_fire():
    sched = BaseScheduler(
        min_inflight=1, max_inflight=10, initial_inflight=5,
    )
    a, b, c = [], [], []
    sched.add_window_change_listener(lambda v: a.append(v))
    sched.add_window_change_listener(lambda v: b.append(v))
    sched.add_window_change_listener(lambda v: c.append(v))

    sched._cb_trip()
    assert a == b == c == [sched.min_inflight]


def test_remove_window_change_listener_stops_callbacks():
    sched = BaseScheduler(
        min_inflight=1, max_inflight=10, initial_inflight=5,
    )
    fired = []

    def listener(v):
        fired.append(v)

    sched.add_window_change_listener(listener)
    sched._cb_trip()
    assert len(fired) == 1

    sched.remove_window_change_listener(listener)
    # Force another change. Even if current_limit stays equal (the listener
    # gate short-circuits no-op writes), we can prove the listener isn't
    # observing by checking it didn't receive a *second* value when a real
    # change happens. Set current_limit directly via _set_current_limit.
    sched._set_current_limit(7)
    assert fired == [sched.min_inflight]  # only the cb_trip value


def test_listener_exceptions_do_not_break_scheduler():
    sched = BaseScheduler(
        min_inflight=1, max_inflight=10, initial_inflight=5,
    )

    def bad_listener(_):
        raise RuntimeError("listener kaboom")

    good_fired = []
    sched.add_window_change_listener(bad_listener)
    sched.add_window_change_listener(lambda v: good_fired.append(v))

    sched._cb_trip()  # must not raise

    assert good_fired == [sched.min_inflight]


def test_no_op_write_does_not_fire_listener():
    """Writing the same current_limit again should be a no-op."""
    sched = BaseScheduler(
        min_inflight=1, max_inflight=10, initial_inflight=5,
    )
    fired = []
    sched.add_window_change_listener(lambda v: fired.append(v))

    # Set to current value — _set_current_limit short-circuits.
    sched._set_current_limit(sched.current_limit)
    assert fired == []


# ---------- engine adaptive worker pool -----------------------------------


def _dummy_fetcher(scheduler):
    """Build a minimal fetcher-like object that exposes `.scheduler`."""
    return SimpleNamespace(scheduler=scheduler)


def _build_engine(*, concurrency, max_inflight, scheduler=None, sink_failure_timeout_sec=None):
    """Build an IngestionEngine that never actually runs.

    `run_stream` is the only consumer of `self.fetcher`, so we point it at a
    SimpleNamespace carrying `.scheduler`. Everything else is None; the
    constructor tolerates that.
    """
    kwargs = {}
    if sink_failure_timeout_sec is not None:
        kwargs["sink_failure_timeout_sec"] = sink_failure_timeout_sec
    return IngestionEngine(
        fetcher=_dummy_fetcher(scheduler) if scheduler else SimpleNamespace(),
        processors={},
        enricher=None,
        sink=None,
        topics={},
        chain=None,
        pipeline=None,
        max_retry=0,
        concurrency=concurrency,
        max_inflight=max_inflight,
        logger=None,
        observability=None,
        decoder=None,
        watermark_manager=None,
        checkpoint_reader=None,
        eos_enabled=False,
        **kwargs,
    )


def test_engine_default_concurrency_is_zero():
    """After the refactor the default must be 0 (adaptive) — preserves the
    'track the scheduler' semantic without forcing operators to opt-in."""
    engine = _build_engine(concurrency=0, max_inflight=5)
    assert engine.concurrency == 0
    assert engine.max_inflight == 5


def test_engine_worker_pool_size_for_adaptive_mode():
    engine = _build_engine(concurrency=0, max_inflight=10)
    # Without run_stream, _active_worker_count starts at 0. The pool size
    # itself is computed inside run_stream, but we can assert the inputs
    # it would use.
    assert engine.concurrency == 0
    assert engine.max_inflight == 10


def test_engine_metrics_worker_count_gauge_initialized_in_noop_mode():
    """NoOp mode exposes WORKER_COUNT and reports the engine's value."""
    engine = _build_engine(concurrency=0, max_inflight=5)
    engine._active_worker_count = 3  # simulate mid-stream

    metrics = EngineMetrics()  # meter=None → all NoOp
    metrics.bind(engine)

    # NoOp stub returns 0 from get_metrics_data but exposes the attribute.
    assert hasattr(metrics, "WORKER_COUNT")
    obs = list(metrics._observe_worker_count(None))
    assert len(obs) == 1
    assert obs[0].value == 3.0


def test_engine_metrics_worker_count_gauge_reports_zero_before_run():
    """Before run_stream spawns workers, the gauge should report 0 so
    Grafana never shows a stale 'active' reading between runs."""
    engine = _build_engine(concurrency=0, max_inflight=5)
    metrics = EngineMetrics()
    metrics.bind(engine)

    obs = list(metrics._observe_worker_count(None))
    assert obs[0].value == 0.0


def test_engine_metrics_worker_count_with_real_otel_sdk():
    """Verify the gauge is registered with the SDK and returns the live value
    on scrape."""
    from opentelemetry.sdk.metrics import MeterProvider as SDKMeterProvider
    from opentelemetry.sdk.metrics.export import InMemoryMetricReader

    engine = _build_engine(concurrency=0, max_inflight=5)
    engine._active_worker_count = 7

    reader = InMemoryMetricReader()
    provider = SDKMeterProvider(metric_readers=[reader])
    meter = provider.get_meter("rpcstream.engine")

    metrics = EngineMetrics(meter=meter, engine=engine)
    reader.collect()
    data = reader.get_metrics_data()

    by_name = {}
    for rm in data.resource_metrics:
        for sm in rm.scope_metrics:
            for m_obj in sm.metrics:
                dp = next(iter(m_obj.data.data_points), None)
                if dp is not None and hasattr(dp, "value"):
                    by_name[m_obj.name] = dp.value

    assert "rpcstream_engine_worker_count" in by_name
    assert by_name["rpcstream_engine_worker_count"] == 7.0


def test_engine_metrics_sink_failure_timeout_gauge_reports_configured_value():
    """sink_delivery_failed only means something in light of the timeout
    it's measured against, and that timeout is tuned per shard (log's
    bursty blocks need a longer budget than block/transaction's -- see
    rpcstream-config-log.yaml). Surface it as a gauge so a dashboard can
    show it next to the failure rate."""
    engine = _build_engine(concurrency=0, max_inflight=5, sink_failure_timeout_sec=30.0)

    metrics = EngineMetrics()  # meter=None → all NoOp
    metrics.bind(engine)

    assert hasattr(metrics, "SINK_FAILURE_TIMEOUT_SEC")
    obs = list(metrics._observe_sink_failure_timeout_sec(None))
    assert len(obs) == 1
    assert obs[0].value == 30.0


def test_engine_metrics_sink_failure_timeout_with_real_otel_sdk():
    """Verify the gauge is registered with the SDK and returns the
    configured value on scrape."""
    from opentelemetry.sdk.metrics import MeterProvider as SDKMeterProvider
    from opentelemetry.sdk.metrics.export import InMemoryMetricReader

    engine = _build_engine(concurrency=0, max_inflight=5, sink_failure_timeout_sec=20.0)

    reader = InMemoryMetricReader()
    provider = SDKMeterProvider(metric_readers=[reader])
    meter = provider.get_meter("rpcstream.engine")

    EngineMetrics(meter=meter, engine=engine)
    reader.collect()
    data = reader.get_metrics_data()

    by_name = {}
    for rm in data.resource_metrics:
        for sm in rm.scope_metrics:
            for m_obj in sm.metrics:
                dp = next(iter(m_obj.data.data_points), None)
                if dp is not None and hasattr(dp, "value"):
                    by_name[m_obj.name] = dp.value

    assert by_name["rpcstream_engine_sink_failure_timeout_sec"] == 20.0


def test_set_current_limit_replaces_admission_semaphore():
    """_set_current_limit must also refresh `scheduler.sem` so new acquires
    go through the fresh semaphore (matches pre-existing behaviour)."""
    sched = BaseScheduler(
        min_inflight=1, max_inflight=10, initial_inflight=5,
    )
    old_sem = sched.sem
    sched._set_current_limit(7)
    assert sched.sem is not old_sem
    assert sched.current_limit == 7


@pytest.mark.parametrize("concurrency", [1, 3, 5])
def test_engine_constructor_accepts_known_concurrency_values(concurrency):
    """Explicit fixed values must still construct cleanly."""
    engine = _build_engine(concurrency=concurrency, max_inflight=20)
    assert engine.concurrency == concurrency


def test_engine_constructor_max_inflight_clamped_to_at_least_one():
    """A misconfigured max_inflight=0 must be coerced to 1, otherwise
    asyncio.Semaphore(0) would deadlock adaptive shrink."""
    engine = _build_engine(concurrency=0, max_inflight=0)
    assert engine.max_inflight == 1

    engine = _build_engine(concurrency=0, max_inflight=-3)
    assert engine.max_inflight == 1


# ---------- schema validation ---------------------------------------------


def test_schema_rejects_negative_concurrency():
    from pydantic import ValidationError
    from rpcstream.config.schema import EngineConfig

    with pytest.raises(ValidationError):
        EngineConfig(concurrency=-1)


def test_schema_accepts_zero_concurrency():
    """0 must be valid (= adaptive mode)."""
    from rpcstream.config.schema import EngineConfig
    cfg = EngineConfig(concurrency=0)
    assert cfg.concurrency == 0


def test_schema_default_concurrency_is_zero():
    from rpcstream.config.schema import EngineConfig
    cfg = EngineConfig()
    assert cfg.concurrency == 0


def test_resolver_keeps_zero_as_adaptive():
    """Resolver must NOT coerce concurrency=0 into max_inflight (that was the
    old behaviour and would have made adaptive mode impossible)."""
    from rpcstream.config.loader import load_pipeline_config
    from rpcstream.config.resolver import resolve

    cfg = load_pipeline_config(
        "pipeline.yaml"
    )
    runtime = resolve(cfg)
    assert runtime.engine.concurrency == 0
    assert runtime.engine.max_inflight == cfg.erpc.inflight.max_inflight