"""Queue-wait is the primary congestion signal, not single-request latency.

Regression tests for the BSC ingestion_lag blow-up: heavy requests (receipt/log
with ~800 payloads) are intrinsically ~1s even when the upstream is healthy, so
a single request's rpc_latency must NOT collapse the inflight window. Only a
sustained queue wait (requests piling up behind the admission slot) is real
congestion. A single latency spike must also be debounced.
"""

import pytest

from rpcstream.scheduler.adaptive import AdaptiveRpcScheduler


def _make_scheduler(**kw):
    kw.setdefault("min_inflight", 1)
    kw.setdefault("max_inflight", 20)
    kw.setdefault("initial_inflight", 10)
    return AdaptiveRpcScheduler(None, **kw)


def _feed(scheduler, latency, queue_wait):
    """Simulate one request outcome with both latency and queue wait."""
    scheduler._update_latency(latency)
    scheduler._update_queue_wait(queue_wait)
    scheduler._adjust_window(True)


def test_heavy_request_high_latency_no_queue_wait_does_not_collapse():
    # Mirrors BSC: receipt/log ~1.2s intrinsic, but the upstream is healthy so
    # nothing queues. With latency_target_ms=0 (fully adaptive, NO per-chain
    # constant) the controller must learn the cheap-call floor (~450ms) and treat
    # the heavy ~1.2s calls as healthy headroom, not congestion.
    scheduler = _make_scheduler(latency_target_ms=0)
    # first establish the cheap-call floor via block/tx-like light traffic
    for _ in range(5):
        _feed(scheduler, 450, 5.0)
    assert scheduler.current_limit > scheduler.min_inflight
    before = scheduler.current_limit
    # sustained heavy-but-healthy requests: high latency, zero queue wait
    for _ in range(40):
        _feed(scheduler, 1250, 5.0)
    # No collapse: heavy requests alone (no queueing) must not shrink the window.
    assert scheduler.current_limit >= before
    assert scheduler.current_limit == scheduler.max_inflight


def test_low_latency_env_self_tunes_without_any_constant():
    # A 50-300ms environment, fully adaptive (latency_target_ms=0). The floor must
    # track the ~50ms best case, the target becomes ~150ms, and the window must
    # still ramp to max on healthy traffic — proving zero per-chain tuning needed.
    scheduler = _make_scheduler(latency_target_ms=0)
    for _ in range(30):
        _feed(scheduler, 50, 2.0)
    assert scheduler.current_limit == scheduler.max_inflight
    # target is derived from the learned ~50ms floor, not a hard-coded constant
    assert scheduler.effective_target_ms() < 300
    # a genuinely 6x-slower spike (300ms vs 50ms floor) with queueing -> shrinks
    for _ in range(20):
        _feed(scheduler, 300, 400.0)
    assert scheduler.current_limit < scheduler.max_inflight


def test_sustained_queue_wait_triggers_shrink():
    scheduler = _make_scheduler(latency_target_ms=0)
    for _ in range(30):
        _feed(scheduler, 450, 5.0)  # ramp up healthy to max
    assert scheduler.current_limit == scheduler.max_inflight
    # Upstream saturates: requests wait far above the queue budget (~180ms).
    for _ in range(20):
        _feed(scheduler, 500, 600.0)
    assert scheduler.current_limit < scheduler.max_inflight


def test_single_latency_spike_is_debounced():
    # One heavy request must not collapse the window on its own.
    scheduler = _make_scheduler(latency_target_ms=0, adjust_cooldown_windows=3)
    for _ in range(5):
        _feed(scheduler, 450, 5.0)
    before = scheduler.current_limit
    # a single spike (high latency + brief queue wait), then immediately healthy
    _feed(scheduler, 4000, 50.0)
    _feed(scheduler, 450, 5.0)
    _feed(scheduler, 450, 5.0)
    _feed(scheduler, 450, 5.0)
    assert scheduler.current_limit >= before  # debounced, no collapse


def test_recovers_quickly_after_queue_clears():
    scheduler = _make_scheduler(latency_target_ms=0)
    for _ in range(30):
        _feed(scheduler, 450, 5.0)
    assert scheduler.current_limit == scheduler.max_inflight
    for _ in range(20):
        _feed(scheduler, 500, 600.0)  # saturate
    low = scheduler.current_limit
    assert low < scheduler.max_inflight
    for _ in range(60):
        _feed(scheduler, 450, 5.0)  # queue clears, healthy
    assert scheduler.current_limit == scheduler.max_inflight
