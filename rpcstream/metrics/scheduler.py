from opentelemetry.metrics import Observation


class _NoOp:
    def add(self, *args, **kwargs):
        pass

    def record(self, *args, **kwargs):
        pass


_STATE_MAP = {"closed": 0, "half_open": 1, "open": 2}


class SchedulerMetrics:
    """Exposes the circuit breaker state to metrics so operators can alert on
    upstream/sink outages (state=2 -> open/tripped)."""

    def __init__(self, meter=None, scheduler=None):
        self._scheduler = scheduler
        if meter is None:
            self.STATE = _NoOp()
            self.FAILURE_RATE = _NoOp()
            self.CONSECUTIVE = _NoOp()
            return

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

    def bind(self, scheduler):
        self._scheduler = scheduler

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
