import asyncio
import random
import time

# Circuit breaker states
CB_CLOSED = "closed"
CB_HALF_OPEN = "half_open"
CB_OPEN = "open"


class BaseScheduler:
    """
    Base scheduler:
    - adaptive logical window
    - semaphore as hard cap
    - EMA telemetry
    - failure-aware circuit breaker (trip/pause on sustained errors)
    """

    def __init__(
        self,
        min_inflight=5,
        max_inflight=50,
        initial_inflight=10,
        latency_target_ms=0,
        target_multiplier=3.0,
        circuit_breaker_enabled=True,
        trip_consecutive_failures=5,
        trip_failure_rate=0.5,
        backoff_base_sec=1.0,
        backoff_max_sec=30.0,
        probe_budget=3,
    ):
        self.min_inflight = max(1, int(min_inflight))
        self.max_inflight = max(self.min_inflight, int(max_inflight))
        self.current_limit = min(
            self.max_inflight,
            max(self.min_inflight, int(initial_inflight)),
        )
        # Optional absolute floor. 0 (default) means "adaptive only" — the
        # effective target is derived from the observed provider latency, so no
        # per-chain tuning is required. A non-zero value adds a hard floor on
        # top of the adaptive target.
        self.latency_target_ms = latency_target_ms
        # Ratio applied to the learned latency floor to obtain the effective
        # target. Chain-independent, so it never needs per-environment tuning.
        self.target_multiplier = target_multiplier

        # hard cap only
        self.sem = asyncio.Semaphore(self.max_inflight)

        self.inflight = 0
        self.success = 0
        self.errors = 0

        self.queue_wait_ema = None
        self.latency_ema = None
        self.alpha = 0.2

        # Learned intrinsic ("best-case") provider latency in ms. Drives the
        # adaptive target so the controller only throttles on genuine latency
        # inflation rather than on a provider that is merely slow by nature.
        self.latency_floor = None
        self.floor_alpha = 0.2

        # ---- Circuit breaker ----
        # When the upstream is unhealthy, firehosing RPCs only wastes CPU and
        # generates a flood of failed work (DLQ/checkpoint). The breaker trips
        # on sustained failures, collapses concurrency to min_inflight AND
        # pauses admission (via _acquire_slot) for an exponentially backed-off
        # cooldown, then probes (half-open) before resuming.
        self.cb_enabled = bool(circuit_breaker_enabled)
        self.cb_trip_consecutive = max(1, int(trip_consecutive_failures))
        self.cb_trip_rate = max(0.0, min(1.0, float(trip_failure_rate)))
        self.cb_backoff_base = max(0.1, float(backoff_base_sec))
        self.cb_backoff_max = max(self.cb_backoff_base, float(backoff_max_sec))
        self.cb_probe_budget = max(1, int(probe_budget))
        self.cb_state = CB_CLOSED
        self.cb_consecutive_failures = 0
        self.cb_failure_rate_ema = 0.0
        self.cb_alpha = 0.3
        self.cb_attempt = 0
        self.cb_cooldown_until = 0.0
        self.cb_probes_remaining = 0
        self.cb_probe_success = 0

        self.start_ts = time.time()

    async def _acquire_slot(self):
        while True:
            # Circuit breaker: while OPEN, wait out the cooldown (yield to the
            # event loop so we don't busy-spin) before transitioning to a
            # half-open probe. This is what stops the failure-loop from
            # saturating CPU/downstream during an upstream outage.
            if self.cb_enabled and self.cb_state == CB_OPEN:
                now = time.monotonic()
                if now < self.cb_cooldown_until:
                    await asyncio.sleep(min(0.05, self.cb_cooldown_until - now))
                    continue
                self._cb_half_open()

            if self.inflight >= self.current_limit:
                await asyncio.sleep(0.001)
                continue

            # In half-open, only admit up to probe_budget requests; once
            # exhausted, wait for the in-flight probes to resolve (the outcome
            # handler will CLOSE or re-OPEN the breaker).
            if self.cb_enabled and self.cb_state == CB_HALF_OPEN:
                if self.cb_probes_remaining <= 0:
                    if self.inflight == 0:
                        # Probes finished but state unchanged: re-evaluate.
                        await asyncio.sleep(0.01)
                        continue
                    await asyncio.sleep(0.01)
                    continue
                self.cb_probes_remaining -= 1

            break

        await self.sem.acquire()
        self.inflight += 1

    # ----------------------------
    # Circuit breaker
    # ----------------------------
    def is_tripped(self) -> bool:
        """True when admission is paused (OPEN state). The engine uses this to
        stop pulling new cursors during an upstream outage. Half-open is NOT
        tripped so a few probe fetches are still allowed."""
        return self.cb_enabled and self.cb_state == CB_OPEN

    def _cb_trip(self):
        self.cb_state = CB_OPEN
        self.cb_attempt += 1
        base = self.cb_backoff_base * (2 ** (self.cb_attempt - 1))
        base = min(base, self.cb_backoff_max)
        base *= 1.0 + random.random() * 0.5  # jitter to avoid thundering herd
        self.cb_cooldown_until = time.monotonic() + base
        self.cb_probes_remaining = 0
        self.cb_probe_success = 0
        # Collapse concurrency immediately so we stop hammering the upstream.
        self.current_limit = self.min_inflight

    def _cb_half_open(self):
        self.cb_state = CB_HALF_OPEN
        self.cb_probes_remaining = self.cb_probe_budget
        self.cb_probe_success = 0

    def _cb_close(self):
        self.cb_state = CB_CLOSED
        self.cb_attempt = 0
        self.cb_consecutive_failures = 0
        self.cb_failure_rate_ema = 0.0
        self.cb_probes_remaining = 0
        self.cb_probe_success = 0

    def _record_outcome(self, success: bool) -> None:
        """Feed a request outcome into the breaker. Must only be called for
        non-`expected_warning` failures (same gate as `_adjust_window`), so
        benign 'block not ready' propagation gaps don't trip the breaker."""
        if not self.cb_enabled:
            return

        if success:
            self.cb_consecutive_failures = 0
            self.cb_failure_rate_ema = (1 - self.cb_alpha) * self.cb_failure_rate_ema
            if self.cb_state == CB_HALF_OPEN:
                self.cb_probe_success += 1
                if self.cb_probe_success >= self.cb_probe_budget:
                    self._cb_close()
            return

        # failure
        self.cb_consecutive_failures += 1
        self.cb_failure_rate_ema = (
            self.cb_alpha * 1.0 + (1 - self.cb_alpha) * self.cb_failure_rate_ema
        )
        if self.cb_state == CB_HALF_OPEN:
            self._cb_trip()  # a probe failed -> reopen with longer backoff
        elif self.cb_state == CB_CLOSED:
            if (
                self.cb_consecutive_failures >= self.cb_trip_consecutive
                or self.cb_failure_rate_ema >= self.cb_trip_rate
            ):
                self._cb_trip()
        # if already OPEN, stay open (cooldown unchanged)

    def _release_slot(self):
        self.inflight -= 1
        self.sem.release()

    def _update_latency(self, latency):
        if self.latency_ema is None:
            self.latency_ema = latency
        else:
            self.latency_ema = self.alpha * latency + (1 - self.alpha) * self.latency_ema
        self._update_floor(latency)

    def _update_floor(self, latency):
        """Track the provider's intrinsic (best-case) latency.

        The floor is the latency a single in-flight request experiences with no
        self-induced congestion, and is used as the basis for the adaptive
        target. Two rules keep it honest:

        * When concurrency is at its minimum there is no self-congestion, so the
          observed latency is the true idle latency — track it freely.
        * Under load the floor may only IMPROVE (move down). A congestion spike
          must never inflate the baseline, otherwise the controller would stop
          protecting the upstream during a sustained slowdown.
        """
        if self.latency_floor is None:
            self.latency_floor = latency
            return
        if self.current_limit <= self.min_inflight:
            self.latency_floor = (
                self.floor_alpha * latency + (1 - self.floor_alpha) * self.latency_floor
            )
        elif latency < self.latency_floor:
            self.latency_floor = (
                self.floor_alpha * latency + (1 - self.floor_alpha) * self.latency_floor
            )

    def effective_target_ms(self):
        """Adaptive latency target, independent of any fixed per-chain value.

        effective_target = max(latency_floor * target_multiplier, latency_target_ms)
        When latency_target_ms is 0 the target is purely adaptive.
        """
        floor = self.latency_floor if self.latency_floor is not None else (self.latency_ema or 0.0)
        adaptive = floor * self.target_multiplier
        if self.latency_target_ms and self.latency_target_ms > 0:
            return max(adaptive, float(self.latency_target_ms))
        return adaptive

    def _update_queue_wait(self, wait_ms):
        if self.queue_wait_ema is None:
            self.queue_wait_ema = wait_ms
        else:
            self.queue_wait_ema = self.alpha * wait_ms + (1 - self.alpha) * self.queue_wait_ema
