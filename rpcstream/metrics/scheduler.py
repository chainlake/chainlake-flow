from opentelemetry.metrics import Observation


class _NoOp:
    def add(self, *args, **kwargs):
        pass

    def record(self, *args, **kwargs):
        pass


_STATE_MAP = {"closed": 0, "half_open": 1, "open": 2}


class SchedulerMetrics:
    """Exposes the circuit breaker state and the adaptive latency controller
    state to metrics so operators can alert on upstream/sink outages and watch
    the self-tuning window over time in Grafana.
    """

    def __init__(self, meter=None, scheduler=None):
        self._scheduler = scheduler
        if meter is None:
            self.STATE = _NoOp()
            self.FAILURE_RATE = _NoOp()
            self.CONSECUTIVE = _NoOp()
            self.LATENCY_FLOOR_MS = _NoOp()
            self.LATENCY_EMA_MS = _NoOp()
            self.EFFECTIVE_TARGET_MS = _NoOp()
            self.CURRENT_LIMIT = _NoOp()
            return

        # Circuit breaker
        self.STATE = meter.create_observable_gauge(
            "rpcstream_scheduler_circuit_breaker_state",
            description="circuit breaker state: 0=closed, 1=half_open, 2=open",
            callbacks=[self._observe_state],
        )
        self.FAILURE_RATE = meter.create_observable_gauge(
            "rpcstream_scheduler_circuit_breaker_failure_rate",
            description="EMA of recent request failure ratio (0..1)",
            callbacks=[self._observe_failrate],
        )
        self.CONSECUTIVE = meter.create_observable_gauge(
            "rpcstream_scheduler_circuit_breaker_consecutive_failures",
            description="consecutive non-expected failures since last success",
            callbacks=[self._observe_consecutive],
        )

        # Adaptive latency controller (instrumented at _update_latency /
        # _update_floor). These gauges surface the values that drive the
        # effective_target_ms formula so Grafana can plot floor / EMA / target
        # alongside the actual observed RPC latency.
        self.LATENCY_FLOOR_MS = meter.create_observable_gauge(
            "rpcstream_scheduler_latency_floor_ms",
            unit="ms",
            description=(
                "Learned intrinsic (best-case) provider latency in ms. Only "
                "improves over time, never inflates under self-induced load."
            ),
            callbacks=[self._observe_latency_floor],
        )
        self.LATENCY_EMA_MS = meter.create_observable_gauge(
            "rpcstream_scheduler_latency_ema_ms",
            unit="ms",
            description=(
                "EMA of observed RPC latency in ms. Falls back when the "
                "floor is unavailable (cold start)."
            ),
            callbacks=[self._observe_latency_ema],
        )
        self.EFFECTIVE_TARGET_MS = meter.create_observable_gauge(
            "rpcstream_scheduler_effective_target_ms",
            unit="ms",
            description=(
                "Effective latency target = max(latency_floor * target_multiplier, "
                "latency_target_ms). When latency_target_ms is 0 this is purely "
                "adaptive."
            ),
            callbacks=[self._observe_effective_target],
        )
        self.CURRENT_LIMIT = meter.create_observable_gauge(
            "rpcstream_scheduler_current_limit",
            description=(
                "Current adaptive inflight window (between min_inflight and "
                "max_inflight). Drops on failures / latency inflation; grows "
                "when latency stays under target."
            ),
            callbacks=[self._observe_current_limit],
        )

    def bind(self, scheduler):
        self._scheduler = scheduler

    # -- circuit breaker observers ----------------------------------------

    def _observe_state(self, options):
        if self._scheduler is None:
            return
        yield Observation(value=_STATE_MAP.get(self._scheduler.cb_state, 0))

    def _observe_failrate(self, options):
        if self._scheduler is None:
            return
        yield Observation(value=float(self._scheduler.cb_failure_rate_ema))

    def _observe_consecutive(self, options):
        if self._scheduler is None:
            return
        yield Observation(value=float(self._scheduler.cb_consecutive_failures))

    # -- adaptive latency observers ---------------------------------------

    def _observe_latency_floor(self, options):
        if self._scheduler is None:
            return
        yield Observation(value=float(self._scheduler.latency_floor or 0.0))

    def _observe_latency_ema(self, options):
        if self._scheduler is None:
            return
        yield Observation(value=float(self._scheduler.latency_ema or 0.0))

    def _observe_effective_target(self, options):
        if self._scheduler is None:
            return
        try:
            yield Observation(value=float(self._scheduler.effective_target_ms()))
        except Exception:
            # effective_target_ms can transiently raise before any sample has
            # been recorded. Yield 0 rather than failing the entire scrape.
            yield Observation(value=0.0)

    def _observe_current_limit(self, options):
        if self._scheduler is None:
            return
        yield Observation(value=float(self._scheduler.current_limit))