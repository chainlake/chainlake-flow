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
            self.DLQ_RETRY_COUNTER = _NoOp()
            self.DLQ_RESOLVED_COUNTER = _NoOp()
            self.BLOCK_LATENCY = _NoOp()
            self.QUEUE_WAIT = _NoOp()
            self.TOTAL_TIME = _NoOp()
            self.INFLIGHT = _NoOp()
            self.ERROR_COUNTER = _NoOp()
            self.CHAIN_LAG = _NoOp()
            self.INGESTION_LAG = _NoOp()
            self.INGESTION_LAG_MS = _NoOp()
            self.WORKER_COUNT = _NoOp()
            self.SINK_FAILURE_TIMEOUT_SEC = _NoOp()
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

        # DLQ retry worker outcomes (retry_dlq_record / mark_dlq_resolved).
        # Labeled by outcome=success|failed so a single counter covers both;
        # only ever incremented by the rpcstream-dlq-retry process, never the
        # main engine, since only that process calls retry_dlq_record.
        self.DLQ_RETRY_COUNTER = meter.create_counter(
            "rpcstream_engine_dlq_retry_total",
        )

        self.DLQ_RESOLVED_COUNTER = meter.create_counter(
            "rpcstream_engine_dlq_resolved_total",
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

        # Static per-process config surfaced as a gauge so a dashboard can
        # show "how much slack does this shard have" next to the sink
        # delivery failure rate -- sink_delivery_failed only means something
        # in light of the timeout it's measured against, and that timeout is
        # deliberately tuned per shard (see rpcstream-config-log.yaml: log's
        # bursty blocks need a longer budget than block/transaction's).
        self.SINK_FAILURE_TIMEOUT_SEC = meter.create_observable_gauge(
            "rpcstream_engine_sink_failure_timeout_sec",
            unit="s",
            description="Configured engine.sink_failure_timeout_sec for this process.",
            callbacks=[self._observe_sink_failure_timeout_sec],
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

    def _observe_sink_failure_timeout_sec(self, options):
        if self._engine is None:
            return
        yield Observation(value=float(getattr(self._engine, "sink_failure_timeout_sec", 0)))