from opentelemetry.metrics import Observation


class _NoOp:
    def add(self, *args, **kwargs):
        pass

    def record(self, *args, **kwargs):
        pass


class EngineMetrics:
    def __init__(self, meter=None, engine=None):
        # `engine` is an optional weak reference target so the WORKER_COUNT
        # observable gauge can pull the live active-worker count at scrape
        # time without forcing callers to call `bind(engine)` after the fact.
        self._engine = engine
        if meter is None:
            self.BLOCK_COUNTER = _NoOp()
            self.ROW_COUNTER = _NoOp()
            self.DLQ_COUNTER = _NoOp()
            self.BLOCK_LATENCY = _NoOp()
            self.QUEUE_WAIT = _NoOp()
            self.TOTAL_TIME = _NoOp()
            self.INFLIGHT = _NoOp()
            self.ERROR_COUNTER = _NoOp()
            self.CHAIN_LAG = _NoOp()
            self.INGESTION_LAG = _NoOp()
            self.INGESTION_LAG_MS = _NoOp()
            self.WORKER_COUNT = _NoOp()
            return

        # Throughput
        self.BLOCK_COUNTER = meter.create_counter(
            "rpcstream_engine_blocks_total",
        )

        self.ROW_COUNTER = meter.create_counter(
            "rpcstream_engine_rows_total",
        )

        self.DLQ_COUNTER = meter.create_counter(
            "rpcstream_engine_dlq_total",
        )

        # Latency
        self.BLOCK_LATENCY = meter.create_histogram(
            "rpcstream_engine_block_latency_ms",
            unit="ms",
        )

        self.QUEUE_WAIT = meter.create_histogram(
            "rpcstream_engine_queue_wait_ms",
            unit="ms",
        )

        self.TOTAL_TIME = meter.create_histogram(
            "rpcstream_engine_total_time_ms",
            unit="ms",
        )

        # Load
        self.INFLIGHT = meter.create_up_down_counter(
            "rpcstream_engine_inflight",
        )

        # Errors
        self.ERROR_COUNTER = meter.create_counter(
            "rpcstream_engine_errors_total",
        )

        # lag
        self.CHAIN_LAG = meter.create_histogram(
            name="rpcstream_engine_chain_lag",
            description="point-in-time lag at processing moment",
        )

        self.INGESTION_LAG = meter.create_histogram(
            name="rpcstream_engine_ingestion_lag",
            description="TRUE pipeline lag (monotonic), in blocks",
        )

        self.INGESTION_LAG_MS = meter.create_histogram(
            name="rpcstream_engine_ingestion_lag_ms",
            description="wall-clock lag between block timestamp and processing time",
            unit="ms",
        )

        # Adaptive worker pool visibility — pairs with
        # rpcstream_scheduler_current_limit / _effective_target_ms so Grafana
        # can plot "are workers starving the inflight window?".
        self.WORKER_COUNT = meter.create_observable_gauge(
            "rpcstream_engine_worker_count",
            description=(
                "Live cursor-fetching worker count. In adaptive mode "
                "(engine.concurrency == 0) this tracks "
                "erpc.inflight.current_limit, bounded by max_inflight. "
                "In fixed mode this equals engine.concurrency."
            ),
            callbacks=[self._observe_worker_count],
        )

    def bind(self, engine):
        """Attach the engine instance so WORKER_COUNT can read its live
        _active_worker_count. Optional — the constructor accepts it too."""
        self._engine = engine

    def _observe_worker_count(self, options):
        if self._engine is None:
            return
        # Report 0 before run_stream() spawns workers and after it ends so
        # the gauge never publishes a stale "active" reading between runs.
        yield Observation(value=float(getattr(self._engine, "_active_worker_count", 0)))