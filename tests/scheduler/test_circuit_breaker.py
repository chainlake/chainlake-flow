import asyncio

from rpcstream.scheduler.base import BaseScheduler, CB_CLOSED, CB_HALF_OPEN, CB_OPEN


def make_sched(**kwargs):
    kw = dict(
        min_inflight=1,
        max_inflight=10,
        initial_inflight=5,
        latency_target_ms=100,
    )
    kw.update(kwargs)
    return BaseScheduler(**kw)


def test_disabled_never_trips():
    s = make_sched(circuit_breaker_enabled=False)
    for _ in range(100):
        s._record_outcome(False)
    assert s.cb_state == CB_CLOSED
    assert s.is_tripped() is False


def test_consecutive_failures_trip_and_collapse_window():
    s = make_sched(trip_consecutive_failures=5, trip_failure_rate=0.99)
    for _ in range(4):
        s._record_outcome(False)
        assert s.cb_state == CB_CLOSED
    s._record_outcome(False)
    assert s.cb_state == CB_OPEN
    assert s.is_tripped() is True
    # concurrency collapses to min_inflight on trip
    assert s.current_limit == s.min_inflight
    assert s.cb_attempt == 1


def test_failure_rate_trips():
    s = make_sched(trip_consecutive_failures=100, trip_failure_rate=0.5)
    s.cb_alpha = 0.3
    # EMA: 0 -> 0.3 -> 0.51 (>= 0.5) after two failures
    s._record_outcome(False)
    assert s.cb_state == CB_CLOSED
    s._record_outcome(False)
    assert s.cb_state == CB_OPEN
    assert s.is_tripped() is True


def test_expected_warnings_do_not_count():
    # _record_outcome is only called for non-expected failures, but verify that
    # feeding a benign "success-like" path keeps counters clean.
    s = make_sched(trip_consecutive_failures=2)
    s._record_outcome(True)
    assert s.cb_consecutive_failures == 0
    s._record_outcome(False)
    s._record_outcome(True)  # success resets consecutive failure counter
    assert s.cb_consecutive_failures == 0
    assert s.cb_state == CB_CLOSED


def test_success_resets_failure_ema():
    s = make_sched(trip_consecutive_failures=5, trip_failure_rate=0.99)
    s.cb_alpha = 0.3
    s._record_outcome(False)
    s._record_outcome(False)  # ema ~0.51
    assert s.cb_failure_rate_ema > 0.5
    s._record_outcome(True)
    assert s.cb_failure_rate_ema < 0.5
    assert s.cb_consecutive_failures == 0


def test_half_open_probes_succeed_closes_breaker():
    s = make_sched(trip_consecutive_failures=2, probe_budget=3)
    s._record_outcome(False)
    s._record_outcome(False)
    assert s.cb_state == CB_OPEN

    # Cooldown elapsed -> next acquisition moves to half-open.
    s.cb_cooldown_until = 0.0

    async def run():
        for _ in range(3):
            await s._acquire_slot()  # consumes a probe slot
            s._record_outcome(True)  # probe success
            s._release_slot()

    asyncio.run(run())
    assert s.cb_state == CB_CLOSED
    assert s.cb_attempt == 0
    assert s.cb_probe_success == 0


def test_half_open_probe_failure_reopens():
    s = make_sched(trip_consecutive_failures=2, probe_budget=3)
    s._record_outcome(False)
    s._record_outcome(False)
    assert s.cb_state == CB_OPEN
    attempt = s.cb_attempt

    s.cb_cooldown_until = 0.0

    async def run():
        await s._acquire_slot()
        s._record_outcome(False)  # a probe failed -> reopen
        s._release_slot()

    asyncio.run(run())
    assert s.cb_state == CB_OPEN
    assert s.cb_attempt == attempt + 1  # backoff escalated


def test_open_admission_blocks_until_cooldown():
    s = make_sched(trip_consecutive_failures=1, probe_budget=3)
    s._record_outcome(False)
    assert s.cb_state == CB_OPEN

    # Cooldown far in the future -> _acquire_slot must yield (not busy-spin).
    s.cb_cooldown_until = 1e18

    async def run():
        task = asyncio.create_task(s._acquire_slot())
        await asyncio.sleep(0.05)
        assert not task.done()
        task.cancel()

    asyncio.run(run())
    # is_tripped gates the engine producer loop
    assert s.is_tripped() is True
