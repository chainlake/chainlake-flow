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
        # Queue-wait budget (ms). The PRIMARY congestion signal is how long a
        # request sits waiting for an admission slot (`_acquire_slot`), NOT the
        # raw rpc_latency of a single (possibly heavy) request. When the observed
        # queue wait stays under this budget the upstream is keeping up and we
        # grow; when it exceeds the budget the upstream is saturated and we
        # shrink. 0 (default) derives the budget from the effective latency
        # target so heavy vs. light chains need no manual tuning.
        queue_wait_target_ms=0,
        # Contiguous windows a signal must persist before we act, so a single
        # heavy-request latency spike or transient blip cannot shrink the window.
        adjust_cooldown_windows=3,
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
        # Optional absolute queue-wait budget (ms). 0 (default) means "adaptive
        # only" — derived from the effective latency target. A non-zero value
        # hard-caps the budget regardless of the learned latency.
        self.queue_wait_target_ms = queue_wait_target_ms
        # Number of contiguous windows a congestion/growth signal must persist
        # before _adjust_window acts, to suppress single-blip over-reaction.
        self.adjust_cooldown_windows = max(1, int(adjust_cooldown_windows))

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
        # Rolling observation of the provider's *best-case* (intrinsic) latency:
        # the lowest latency we have ever seen, with only an extremely slow upward
        # drift so the floor can follow a genuinely faster environment without
        # being inflated by a transient congestion spike. This makes the adaptive
        # target correct for ANY chain/provider (50ms L2 RPCs up to BSC's ~1.2s
        # heavy requests) with latency_target_ms left at 0 — no per-chain tuning.
        self._floor_min = None
        self._floor_recovery = 1.0008  # ~0.08%/request slow upward drift

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

        # Contiguous-window debounce state for _adjust_window. We only SHRINK
        # once a congestion signal (queue wait OR 3x latency) has persisted for
        # `adjust_cooldown_windows` consecutive windows, so a single heavy-request
        # latency spike cannot collapse the inflight window. Growth is not
        # debounced (see adaptive.py), so this counter is only ever used for
        # shrink decisions.
        self._congested_windows = 0

        self.start_ts = time.time()

        # Listeners notified synchronously whenever `current_limit` changes
        # (adaptive grow/shrink, circuit breaker collapse, etc.). Used by the
        # adaptive engine worker pool to track inflight without polling.
        self._window_change_listeners: list = []

    def add_window_change_listener(self, listener):
        """Register a callback fired synchronously when `current_limit` changes.
        The listener receives the new limit. Returns the listener so it can be
        passed back to `remove_window_change_listener` for cleanup.
        """
        self._window_change_listeners.append(listener)
        return listener

    def remove_window_change_listener(self, listener):
        try:
            self._window_change_listeners.remove(listener)
        except ValueError:
            pass

    def _set_current_limit(self, new_limit):
        """Single mutation point: update current_limit, replace the admission
        semaphore, and fire window-change listeners. Callers (cb_trip,
        _adjust_window) must use this instead of writing self.current_limit
        directly so the listeners stay consistent.
        """
        new_limit = max(self.min_inflight, min(self.max_inflight, int(new_limit)))
        if new_limit == self.current_limit:
            return False
        self.current_limit = new_limit
        # Replace the admission semaphore (matches the existing
        # `_on_window_change` pattern: workers holding the old reference
        # release it normally; new acquires go to the fresh semaphore).
        self.sem = asyncio.Semaphore(self.current_limit)
        for listener in list(self._window_change_listeners):
            try:
                listener(self.current_limit)
            except Exception:
                # Listener failures must not break the scheduler.
                pass
        return True

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
        self._set_current_limit(self.min_inflight)

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
        self-induced congestion, and is the basis for the adaptive target. Two
        rules keep it honest across *any* environment (50ms L2 RPCs up to BSC's
        ~1.2s heavy requests), with no per-chain constant:

        * The floor is anchored to the **lowest latency ever observed**
          (`_floor_min`), taken via `min` so a congestion/retry spike can NEVER
          inflate it. A mixed workload (cheap block/tx calls + heavy receipt/log
          calls) therefore keys the target off the cheap calls' intrinsic cost,
          leaving the heavy calls' natural latency well under the target — so a
          merely-slow request is never mistaken for saturation.
        * A per-request slow upward drift (`_floor_recovery`) lets the floor
          follow a genuinely faster environment (e.g. after a provider upgrade
          or a move to a lower-latency RPC) without being stuck on a startup
          outlier forever.
        * When concurrency is at its minimum there is no self-congestion, so we
          additionally EMA toward the raw observation to capture the true idle
          latency precisely.
        """
        # Rolling best-case floor (only ever moves down on observation, drifts up
        # extremely slowly so a faster environment is reflected over time).
        if self._floor_min is None:
            self._floor_min = latency
        else:
            self._floor_min = min(self._floor_min, latency) * self._floor_recovery

        if self.latency_floor is None:
            self.latency_floor = latency
            return
        if self.current_limit <= self.min_inflight:
            # No self-congestion: the observation is the true idle latency. Track
            # it freely (EMA), but never let the slow-drift floor regress below it.
            self.latency_floor = (
                self.floor_alpha * latency + (1 - self.floor_alpha) * self.latency_floor
            )
            self.latency_floor = max(self.latency_floor, self._floor_min / self._floor_recovery)
        else:
            # Under load: only IMPROVE (the lower of the rolling min and current).
            candidate = self._floor_min / self._floor_recovery
            if candidate < self.latency_floor:
                self.latency_floor = candidate

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

    def effective_queue_target_ms(self):
        """Queue-wait budget: how long a request may wait for an admission slot
        before we treat the upstream as saturated.

        The queue wait is the REAL congestion signal (it reflects how many
        requests are piled up behind the inflight window). The single-request
        rpc_latency is NOT a congestion signal by itself — heavy requests
        (e.g. receipt/log with large payloads) are intrinsically slow even when
        the upstream is healthy.

        Budget defaults to a fraction of the effective latency target so it
        scales with chain/provider speed without per-chain tuning; a non-zero
        `queue_wait_target_ms` hard-caps it.
        """
        if self.queue_wait_target_ms and self.queue_wait_target_ms > 0:
            return float(self.queue_wait_target_ms)
        return max(1.0, self.effective_target_ms() * 0.15)

    def _update_queue_wait(self, wait_ms):
        if self.queue_wait_ema is None:
            self.queue_wait_ema = wait_ms
        else:
            self.queue_wait_ema = self.alpha * wait_ms + (1 - self.alpha) * self.queue_wait_ema
