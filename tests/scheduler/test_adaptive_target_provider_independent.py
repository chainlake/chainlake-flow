"""Provider-independent adaptive latency target.

The adaptive target is derived from the observed provider latency floor, so the
controller must NOT collapse concurrency for a chain/provider that is merely
slow by nature (e.g. BSC ~400-900ms) — only for genuine latency inflation or
errors. These tests prove the behaviour is identical across very different
latency baselines without any per-chain constant.
"""

import pytest

from rpcstream.scheduler.adaptive import AdaptiveRpcScheduler


def _drive(scheduler, latencies):
    """Feed a sequence of observed latencies through the controller."""
    for latency in latencies:
        scheduler._update_latency(latency)
        scheduler._adjust_window(True)


def _healthy_phase(baseline, steps=40):
    # 0.9x .. 1.1x around the baseline: normal per-request variance.
    return [baseline * (1.1 if i % 2 else 0.9) for i in range(steps)]


def _congestion_phase(baseline, steps=20):
    # Sustained 4x inflation: a genuinely saturated upstream.
    return [baseline * 4 for _ in range(steps)]


@pytest.mark.parametrize("baseline", [20, 200, 400, 900, 1500])
def test_healthy_traffic_never_collapses_regardless_of_baseline(baseline):
    scheduler = AdaptiveRpcScheduler(
        None,
        min_inflight=1,
        max_inflight=20,
        initial_inflight=10,
        latency_target_ms=0,  # pure adaptive
    )
    _drive(scheduler, _healthy_phase(baseline))
    # Healthy traffic at ANY baseline must ramp to max, not collapse to floor.
    assert scheduler.current_limit == scheduler.max_inflight
    assert scheduler.latency_floor is not None
    # effective target must be well above the baseline (no false throttle)
    assert scheduler.effective_target_ms() > baseline


@pytest.mark.parametrize("baseline", [20, 200, 400, 900, 1500])
def test_congestion_inflation_triggers_protection(baseline):
    scheduler = AdaptiveRpcScheduler(
        None,
        min_inflight=1,
        max_inflight=20,
        initial_inflight=10,
        latency_target_ms=0,
    )
    _drive(scheduler, _healthy_phase(baseline))  # ramp up
    assert scheduler.current_limit == scheduler.max_inflight
    _drive(scheduler, _congestion_phase(baseline))  # saturate
    # Window must have dropped meaningfully under genuine 4x inflation.
    assert scheduler.current_limit <= max(scheduler.min_inflight, scheduler.max_inflight // 2)


def test_recovers_after_congestion_clears():
    baseline = 600
    scheduler = AdaptiveRpcScheduler(
        None, min_inflight=1, max_inflight=20, initial_inflight=10, latency_target_ms=0
    )
    _drive(scheduler, _healthy_phase(baseline))
    _drive(scheduler, _congestion_phase(baseline))
    low = scheduler.current_limit
    assert low <= scheduler.max_inflight // 2
    _drive(scheduler, _healthy_phase(baseline, steps=60))  # congestion clears
    assert scheduler.current_limit == scheduler.max_inflight  # ramps back up


def test_errors_collapse_to_min_inflight():
    # Preserves the existing "all timeouts -> 1" behaviour (user requirement #2).
    scheduler = AdaptiveRpcScheduler(
        None, min_inflight=1, max_inflight=20, initial_inflight=10, latency_target_ms=0
    )
    _drive(scheduler, _healthy_phase(400))
    assert scheduler.current_limit == scheduler.max_inflight
    for _ in range(60):
        scheduler._adjust_window(False)  # simulated upstream errors/timeouts
    assert scheduler.current_limit == scheduler.min_inflight


def test_explicit_absolute_floor_still_honoured():
    # A non-zero latency_target_ms acts as an additional hard floor on top of
    # the adaptive target (opt-in), without breaking the adaptive behaviour.
    scheduler = AdaptiveRpcScheduler(
        None,
        min_inflight=1,
        max_inflight=20,
        initial_inflight=10,
        latency_target_ms=500,
    )
    _drive(scheduler, _healthy_phase(400))
    # baseline 400 -> adaptive target is floor*3 (floor ~360 under 0.9-1.1x
    # variance) ~= 1080; absolute floor 500 is below it, so healthy traffic
    # still ramps to max.
    assert scheduler.current_limit == scheduler.max_inflight
    assert scheduler.effective_target_ms() >= 500
    assert scheduler.latency_floor is not None
    assert scheduler.effective_target_ms() == pytest.approx(scheduler.latency_floor * 3.0)
