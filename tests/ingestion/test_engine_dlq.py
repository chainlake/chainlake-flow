import asyncio
import time
from types import SimpleNamespace

from rpcstream.client.models import RpcErrorResult, RpcTaskMeta
from rpcstream.adapters.evm.enrich import EvmEnricher
from rpcstream.ingestion.engine import IngestionEngine
from rpcstream.state.checkpoint import CheckpointIdentity, WatermarkManager


class DummyFetcher:
    def __init__(self, value, meta=None):
        self.value = value
        self.meta = meta or SimpleNamespace(extra={})

    async def fetch(self, _block_number):
        return {"trace": (self.value, self.meta)}


class FailingTraceProcessor:
    def process(self, _block_number, _value):
        raise TypeError("list indices must be integers or slices, not str")


class SuccessfulTraceProcessor:
    def process(self, block_number, _value):
        return {
            "trace": [
                {
                    "type": "trace",
                    "block_number": block_number,
                    "trace_id": f"{block_number}-root",
                }
            ]
        }


class RecordingSink:
    def __init__(self):
        self.sent = []
        self.sent_transactions = []

    async def start(self):
        return None

    async def close(self):
        return None

    async def send(self, topic, rows, wait_delivery=False):
        self.sent.append((topic, rows, wait_delivery))
        return None

    async def send_transaction(self, topic_rows):
        self.sent_transactions.append(topic_rows)


class ShutdownTrackingSink(RecordingSink):
    def __init__(self):
        super().__init__()
        self.closed = False
        self.close_started = asyncio.Event()
        self.finalize_done = asyncio.Event()

    async def close(self):
        self.close_started.set()
        await self.finalize_done.wait()
        self.closed = True


def build_engine(*, sink, eos_enabled=False):
    return IngestionEngine(
        fetcher=DummyFetcher(value=[]),
        processors={"trace": FailingTraceProcessor()},
        enricher=EvmEnricher(),
        sink=sink,
        topics={"trace": "bsc.raw_trace"},
        dlq_topic="dlq.ingestion",
        chain=SimpleNamespace(type="evm", network_label="bsc-mainnet"),
        pipeline=SimpleNamespace(name="bsc_mainnet_realtime_checkpoint"),
        max_retry=1,
        concurrency=1,
        logger=None,
        watermark_manager=None,
        checkpoint_reader=None,
        eos_enabled=eos_enabled,
    )


def build_success_engine(*, sink, eos_enabled=False):
    return IngestionEngine(
        fetcher=DummyFetcher(value=[]),
        processors={"trace": SuccessfulTraceProcessor()},
        enricher=EvmEnricher(),
        sink=sink,
        topics={"trace": "bsc.raw_trace"},
        dlq_topic="dlq.ingestion",
        chain=SimpleNamespace(type="evm", network_label="bsc-mainnet"),
        pipeline=SimpleNamespace(name="bsc_mainnet_realtime_checkpoint"),
        max_retry=1,
        concurrency=1,
        logger=None,
        watermark_manager=None,
        checkpoint_reader=None,
        eos_enabled=eos_enabled,
    )


def build_backfill_engine(*, sink):
    return IngestionEngine(
        fetcher=DummyFetcher(value=[]),
        processors={"trace": SuccessfulTraceProcessor()},
        enricher=EvmEnricher(),
        sink=sink,
        topics={"trace": "bsc.raw_trace"},
        dlq_topic="dlq.ingestion",
        chain=SimpleNamespace(type="evm", network_label="bsc-mainnet"),
        pipeline=SimpleNamespace(
            name="bsc_mainnet_backfill_10_20",
            mode="backfill",
            end_cursor=20,
        ),
        max_retry=1,
        concurrency=1,
        logger=None,
        watermark_manager=None,
        checkpoint_reader=None,
        eos_enabled=False,
    )


def test_compute_ingestion_lag_ms_uses_block_timestamp_seconds():
    engine = build_success_engine(sink=RecordingSink())

    lag_ms = engine._compute_ingestion_lag_ms(
        {"block": [{"type": "block", "timestamp": 1_700_000_000}]},
        ingestion_timestamp_ms=1_700_000_012_345,
    )

    assert lag_ms == 12_345


def test_compute_ingestion_lag_ms_returns_none_without_block_timestamp():
    engine = build_success_engine(sink=RecordingSink())

    assert engine._compute_ingestion_lag_ms({"trace": [{"block_number": 1}]}) is None


def test_compute_ingestion_lag_ms_uses_block_timestamp_from_accumulated_bundle():
    engine = build_success_engine(sink=RecordingSink())

    lag_ms = engine._compute_ingestion_lag_ms(
        {
            "block": [{"type": "block", "timestamp": 1_700_000_000}],
            "receipt": [{"block_number": 1}],
            "log": [{"block_number": 1}],
        },
        ingestion_timestamp_ms=1_700_000_000_750,
    )

    assert lag_ms == 750


class BackfillCursorSource:
    def __init__(self, start=1, end=100):
        self.current = start
        self.end = end

    async def next_cursor(self):
        if self.current > self.end:
            return None
        cursor = self.current
        self.current += 1
        return cursor


class RetryThenSuccessFetcher:
    def __init__(self, *, fail_attempts: int):
        self.fail_attempts = fail_attempts
        self.calls = 0

    async def fetch(self, _cursor):
        self.calls += 1
        meta = RpcTaskMeta(task_id=self.calls, submit_ts=0.0, extra={"latency_ms": 1})
        if self.calls <= self.fail_attempts:
            rpc_error = RpcErrorResult(
                error="rpc_response_error(method=eth_getBlockReceipts, code=-32603, message=upstream does not have the requested block yet)",
                meta=meta,
                details={
                    "rpc_error_code": -32603,
                    "rpc_error_message": "upstream does not have the requested block yet",
                    "network_id": "evm:56",
                    "project_id": "main",
                    "upstreams_total": 19,
                    "not_ready_upstreams": 19,
                },
                expected=True,
            )
            return {
                "block": ({"type": "block", "block_number": 1}, meta),
                "transaction": ({"type": "transaction", "block_number": 1}, meta),
                "receipt": rpc_error,
            }

        return {
            "block": ({"type": "block", "block_number": 1}, meta),
            "transaction": ({"type": "transaction", "block_number": 1}, meta),
            "receipt": ({"type": "receipt", "block_number": 1}, meta),
        }


class PassthroughProcessor:
    def __init__(self, entity: str):
        self.entity = entity

    def process(self, cursor, value):
        return {
            self.entity: [
                {
                    "type": self.entity,
                    "cursor": cursor,
                    "value": value,
                }
            ]
        }


def test_engine_sends_sink_delivery_failure_to_dlq_with_correct_entity():
    """Live incident: a KafkaException (MSG_SIZE_TOO_LARGE) failed a "log"
    entity's delivery and there was zero forensic trail -- sink-delivery
    failures never reached the DLQ, only processor/rpc-stage ones did.
    _finalize_checkpoint must now route a failed delivery future to the DLQ
    tagged with the entity/topic it actually belongs to."""
    sink = RecordingSink()
    engine = build_success_engine(sink=sink, eos_enabled=False)

    async def run():
        failed_future = asyncio.get_running_loop().create_future()
        failed_future.set_exception(RuntimeError("Message size too large"))
        await engine._finalize_checkpoint(
            123,
            True,
            [failed_future],
            expected_watermark=None,
            delivery_entities=[("log", "bsc.raw_log")],
        )

    asyncio.run(run())

    assert len(sink.sent) == 1
    topic, rows, wait_delivery = sink.sent[0]
    assert topic == "dlq.ingestion"
    assert wait_delivery is False
    record = rows[0]
    assert record["entity"] == "log"
    assert record["cursor"] == 123
    assert record["stage"] == "sink"
    assert record["error_type"] == "RuntimeError"
    assert record["error_message"] == "log: Message size too large"


def test_engine_sends_one_dlq_record_per_cursor_when_multiple_entities_fail():
    """Live incident: a cursor whose 4 entities all timed out delivery
    together (producer backpressure) got 4 separate per-entity DLQ records.
    retry_dlq_record/_run_one always reprocesses and resinks the *whole*
    cursor regardless of which entity a DLQ record names, so retrying each
    of those 4 records replayed the full cursor 4 times -- duplicating
    every entity's row (including bsc.raw_block, which has nothing to do
    with the entity that actually failed) up to 4x. Must collapse to
    exactly one DLQ record per cursor, carrying all failures in context."""
    sink = RecordingSink()
    engine = build_success_engine(sink=sink, eos_enabled=False)

    async def run():
        block_future = asyncio.get_running_loop().create_future()
        block_future.set_exception(RuntimeError("timed out"))
        log_future = asyncio.get_running_loop().create_future()
        log_future.set_exception(RuntimeError("timed out"))
        await engine._finalize_checkpoint(
            123,
            True,
            [block_future, log_future],
            expected_watermark=None,
            delivery_entities=[("block", "bsc.raw_block"), ("log", "bsc.raw_log")],
        )

    asyncio.run(run())

    assert len(sink.sent) == 1
    topic, rows, wait_delivery = sink.sent[0]
    assert topic == "dlq.ingestion"
    record = rows[0]
    assert record["cursor"] == 123
    assert record["stage"] == "sink"
    failures = record["context"]["failures"]
    assert [f["entity"] for f in failures] == ["block", "log"]
    assert [f["topic"] for f in failures] == ["bsc.raw_block", "bsc.raw_log"]


def test_finalize_checkpoint_records_sink_delivery_wait_by_outcome():
    """Answers "is sink_failure_timeout_sec enough?": a histogram of how
    long delivery actually took, labeled success vs timeout, so a
    dashboard can plot p95/p99 next to the SINK_FAILURE_TIMEOUT_SEC gauge
    (rpcstream_engine_sink_failure_timeout_sec) and see whether the real
    distribution is closing in on the configured ceiling. Previously this
    wait time wasn't tracked as a metric at all."""
    from opentelemetry.sdk.metrics import MeterProvider as SDKMeterProvider
    from opentelemetry.sdk.metrics.export import InMemoryMetricReader
    from rpcstream.runtime.observability.context import ObservabilityContext

    reader = InMemoryMetricReader()
    provider = SDKMeterProvider(metric_readers=[reader])
    observability = ObservabilityContext(service_name="test", meter_provider=provider)

    sink = RecordingSink()
    engine = IngestionEngine(
        fetcher=DummyFetcher(value=[]),
        processors={"trace": SuccessfulTraceProcessor()},
        enricher=EvmEnricher(),
        sink=sink,
        topics={"trace": "bsc.raw_trace"},
        dlq_topic="dlq.ingestion",
        chain=SimpleNamespace(type="evm", network_label="bsc-mainnet"),
        pipeline=SimpleNamespace(name="bsc_mainnet_realtime_checkpoint"),
        max_retry=1,
        concurrency=1,
        logger=None,
        observability=observability,
        watermark_manager=None,
        checkpoint_reader=None,
        eos_enabled=False,
        sink_failure_timeout_sec=0.05,
    )

    async def run():
        succeeding = asyncio.get_running_loop().create_future()
        succeeding.set_result({"message_count": 1})
        await engine._finalize_checkpoint(
            1, True, [succeeding], expected_watermark=None, delivery_entities=[("trace", "bsc.raw_trace")]
        )

        never_resolves = asyncio.get_running_loop().create_future()
        await engine._finalize_checkpoint(
            2, True, [never_resolves], expected_watermark=None, delivery_entities=[("trace", "bsc.raw_trace")]
        )

    asyncio.run(run())

    reader.collect()
    data = reader.get_metrics_data()
    points_by_outcome = {}
    for rm in data.resource_metrics:
        for sm in rm.scope_metrics:
            for m_obj in sm.metrics:
                if m_obj.name == "rpcstream_engine_sink_delivery_wait_ms":
                    for dp in m_obj.data.data_points:
                        points_by_outcome[dp.attributes["outcome"]] = dp

    assert "success" in points_by_outcome
    assert "timeout" in points_by_outcome
    # The timed-out wait must reflect the configured timeout (~50ms), not
    # some unrelated/zero value.
    assert points_by_outcome["timeout"].sum >= 40


def test_engine_sends_trace_dlq_record_when_processor_fails():
    sink = RecordingSink()
    engine = build_engine(sink=sink, eos_enabled=False)

    success, delivery_futures, expected_watermark, delivery_entities = asyncio.run(engine._run_one(95281318))

    assert success is False
    assert delivery_futures == []
    assert expected_watermark is None
    assert len(sink.sent) == 1
    topic, rows, wait_delivery = sink.sent[0]
    assert topic == "dlq.ingestion"
    assert wait_delivery is False
    assert len(rows) == 1
    record = rows[0]
    assert record["entity"] == "trace"
    assert record["cursor"] == 95281318
    assert record["stage"] == "processor"
    assert record["error_type"] == "TypeError"
    assert record["error_message"] == "list indices must be integers or slices, not str"
    assert record["status"] == "pending"


def test_engine_sends_trace_dlq_via_transaction_when_eos_enabled():
    sink = RecordingSink()
    engine = build_engine(sink=sink, eos_enabled=True)

    success, delivery_futures, expected_watermark, delivery_entities = asyncio.run(engine._run_one(95281318))

    assert success is False
    assert delivery_futures == []
    assert expected_watermark is None
    assert sink.sent == []
    assert len(sink.sent_transactions) == 1
    topic_rows = sink.sent_transactions[0]
    assert len(topic_rows) == 1
    topic, rows = topic_rows[0]
    assert topic == "dlq.ingestion"
    assert len(rows) == 1
    assert rows[0]["entity"] == "trace"


def test_engine_sends_business_rows_via_transaction_when_eos_enabled_without_checkpoint():
    sink = RecordingSink()
    engine = build_success_engine(sink=sink, eos_enabled=True)

    success, delivery_futures, expected_watermark, delivery_entities = asyncio.run(engine._run_one(95281318))

    assert success is True
    assert delivery_futures == []
    assert expected_watermark is None
    assert sink.sent == []
    assert len(sink.sent_transactions) == 1
    topic_rows = sink.sent_transactions[0]
    assert topic_rows == [
        (
            "bsc.raw_trace",
            [{"type": "trace", "block_number": 95281318, "trace_id": "95281318-root"}],
        )
    ]


def test_engine_retries_upstream_not_ready_before_success(monkeypatch):
    sink = RecordingSink()
    sleep_calls = []

    async def fake_sleep(seconds):
        sleep_calls.append(seconds)

    monkeypatch.setattr("rpcstream.ingestion.engine.asyncio.sleep", fake_sleep)

    engine = IngestionEngine(
        fetcher=RetryThenSuccessFetcher(fail_attempts=2),
        processors={
            "block": PassthroughProcessor("block"),
            "transaction": PassthroughProcessor("transaction"),
            "receipt": PassthroughProcessor("receipt"),
        },
        enricher=None,
        decoder=None,
        sink=sink,
        topics={
            "block": "bsc.raw_block",
            "transaction": "bsc.enriched_transaction",
        },
        dlq_topic="dlq.ingestion",
        chain=SimpleNamespace(
            type="evm",
            name="bsc",
            network="mainnet",
            network_label="bsc-mainnet",
            interval_seconds=0.45,
        ),
        pipeline=SimpleNamespace(name="bsc_mainnet_realtime_checkpoint"),
        max_retry=1,
        concurrency=1,
        logger=None,
        watermark_manager=None,
        checkpoint_reader=None,
        eos_enabled=True,
        upstream_not_ready_max_attempts=3,
    )

    success, delivery_futures, expected_watermark, delivery_entities = asyncio.run(engine._run_one(103151849))

    assert success is True
    assert expected_watermark is None
    assert delivery_futures == []
    assert sleep_calls == [0.45, 0.9]
    assert sink.sent == []
    assert len(sink.sent_transactions) == 1
    assert sink.sent_transactions[0] == [
        (
            "bsc.raw_block",
            [{"type": "block", "cursor": 103151849, "value": {"type": "block", "block_number": 1}}],
        ),
        (
            "bsc.enriched_transaction",
            [{"type": "transaction", "cursor": 103151849, "value": {"type": "transaction", "block_number": 1}}],
        ),
    ]


def test_engine_sends_dlq_after_exhausting_upstream_not_ready_retries(monkeypatch):
    sink = RecordingSink()
    sleep_calls = []

    async def fake_sleep(seconds):
        sleep_calls.append(seconds)

    monkeypatch.setattr("rpcstream.ingestion.engine.asyncio.sleep", fake_sleep)

    engine = IngestionEngine(
        fetcher=RetryThenSuccessFetcher(fail_attempts=99),
        processors={
            "block": PassthroughProcessor("block"),
            "transaction": PassthroughProcessor("transaction"),
            "receipt": PassthroughProcessor("receipt"),
        },
        enricher=None,
        decoder=None,
        sink=sink,
        topics={
            "block": "bsc.raw_block",
            "transaction": "bsc.enriched_transaction",
        },
        dlq_topic="dlq.ingestion",
        chain=SimpleNamespace(
            type="evm",
            name="bsc",
            network="mainnet",
            network_label="bsc-mainnet",
            interval_seconds=0.45,
        ),
        pipeline=SimpleNamespace(name="bsc_mainnet_realtime_checkpoint"),
        max_retry=1,
        concurrency=1,
        logger=None,
        watermark_manager=None,
        checkpoint_reader=None,
        eos_enabled=False,
        upstream_not_ready_max_attempts=3,
    )

    success, delivery_futures, expected_watermark, delivery_entities = asyncio.run(engine._run_one(103151849))

    assert success is False
    assert expected_watermark is None
    assert delivery_futures == []
    assert sleep_calls == [0.45, 0.9]
    assert sink.sent_transactions == []
    assert len(sink.sent) == 1
    topic, rows, wait_delivery = sink.sent[0]
    assert topic == "dlq.ingestion"
    assert wait_delivery is False
    assert rows[0]["entity"] == "receipt"
    assert rows[0]["status"] == "pending"


def test_engine_marks_dlq_resolved_via_transaction_when_eos_enabled():
    sink = RecordingSink()
    engine = build_engine(sink=sink, eos_enabled=True)

    record = {
        "id": "dlq-1",
        "chain": "evm",
        "network": "bsc-mainnet",
        "pipeline": "bsc_mainnet_realtime_checkpoint",
        "entity": "trace",
        "cursor": 95281318,
        "stage": "processor",
        "error_type": "TypeError",
        "error_message": "boom",
        "payload": {},
        "context": {},
        "retry_count": 0,
        "max_retry": 1,
        "status": "pending",
    }

    asyncio.run(engine.mark_dlq_resolved(record))

    assert sink.sent == []
    assert len(sink.sent_transactions) == 1
    topic_rows = sink.sent_transactions[0]
    assert len(topic_rows) == 1
    assert topic_rows[0][0] == "dlq.ingestion"
    assert topic_rows[0][1][0]["status"] == "resolved"


def test_engine_backfill_shutdown_stops_before_draining_entire_range():
    sink = RecordingSink()
    engine = build_backfill_engine(sink=sink)
    shutdown_event = asyncio.Event()
    started = asyncio.Event()
    release = asyncio.Event()
    processed = []

    async def run_one(cursor):
        processed.append(cursor)
        started.set()
        await release.wait()
        return True, [], None, []

    engine._run_one = run_one

    async def run():
        task = asyncio.create_task(engine.run_stream(BackfillCursorSource(1, 20), shutdown_event=shutdown_event))
        await started.wait()
        shutdown_event.set()
        release.set()
        await task

    asyncio.run(run())

    assert processed == [1]


def test_backfill_compute_lag_uses_end_block_as_remaining_work():
    sink = RecordingSink()
    engine = build_backfill_engine(sink=sink)

    latest_block, chain_lag, ingestion_lag = asyncio.run(engine._compute_lag(12))

    assert latest_block is None
    assert chain_lag is None
    assert ingestion_lag == 8


def test_engine_eos_checkpoint_uses_contiguous_watermark():
    sink = RecordingSink()
    identity = CheckpointIdentity(
        pipeline="pipe",
        chain_uid="evm:56",
        chain_type="evm",
        network="mainnet",
        mode="realtime",
        primary_unit="block",
        entities=("trace",),
    )
    watermark_manager = WatermarkManager(
        sink=sink,
        topic="bsc.commit_watermark",
        state_topic="bsc.cursor_state",
        identity=identity,
        initial_cursor=99,
        flush_on_advance=False,
    )
    engine = build_success_engine(sink=sink, eos_enabled=True)
    engine.watermark_manager = watermark_manager

    async def run():
        await watermark_manager.mark_emitted(100)
        await watermark_manager.mark_emitted(101)

        success_101, delivery_futures_101, expected_101, delivery_entities_101 = await engine._run_one(101)
        await engine._finalize_checkpoint(
            101,
            success_101,
            delivery_futures_101,
            expected_watermark=expected_101,
        )

        success_100, delivery_futures_100, expected_100, delivery_entities_100 = await engine._run_one(100)
        await engine._finalize_checkpoint(
            100,
            success_100,
            delivery_futures_100,
            expected_watermark=expected_100,
        )

        return expected_101, expected_100

    expected_101, expected_100 = asyncio.run(run())

    assert expected_101 is None
    assert expected_100 == 101
    assert sink.sent == []
    assert watermark_manager.cursor == 101
    assert len(sink.sent_transactions) == 2
    assert sink.sent_transactions[0][0] == (
        "bsc.raw_trace",
        [{"type": "trace", "block_number": 101, "trace_id": "101-root"}],
    )
    state_topic_101, state_rows_101 = sink.sent_transactions[0][1]
    assert state_topic_101 == "bsc.cursor_state"
    assert len(state_rows_101) == 1
    assert state_rows_101[0]["cursor"] == 101
    assert state_rows_101[0]["status"] == "completed"
    assert sink.sent_transactions[1][0] == (
        "bsc.raw_trace",
        [{"type": "trace", "block_number": 100, "trace_id": "100-root"}],
    )
    checkpoint_topic, checkpoint_rows = sink.sent_transactions[1][1]
    assert checkpoint_topic == "bsc.commit_watermark"
    assert len(checkpoint_rows) == 1
    checkpoint_row = checkpoint_rows[0]
    assert checkpoint_row["cursor"] == 101
    assert checkpoint_row["status"] == "running"
    assert checkpoint_row["pipeline"] == "pipe"
    assert checkpoint_row["chain_uid"] == "evm:56"
    assert checkpoint_row["chain_type"] == "evm"
    assert checkpoint_row["network"] == "mainnet"
    assert checkpoint_row["mode"] == "realtime"
    assert checkpoint_row["primary_unit"] == "block"
    assert checkpoint_row["entities"] == ["trace"]
    assert checkpoint_row["id"] == identity.key
    assert checkpoint_row["kafka_partition_key"] == identity.key
    assert checkpoint_row["updated_at_ms"] > 0


def test_engine_eos_sequential_success_does_not_write_cursor_state():
    sink = RecordingSink()
    identity = CheckpointIdentity(
        pipeline="pipe",
        chain_uid="evm:56",
        chain_type="evm",
        network="mainnet",
        mode="realtime",
        primary_unit="block",
        entities=("trace",),
    )
    watermark_manager = WatermarkManager(
        sink=sink,
        topic="bsc.commit_watermark",
        state_topic="bsc.cursor_state",
        identity=identity,
        initial_cursor=99,
        flush_on_advance=False,
    )
    engine = build_success_engine(sink=sink, eos_enabled=True)
    engine.watermark_manager = watermark_manager

    async def run():
        await watermark_manager.mark_emitted(100)
        success, delivery_futures, expected_watermark, delivery_entities = await engine._run_one(100)
        await engine._finalize_checkpoint(
            100,
            success,
            delivery_futures,
            expected_watermark=expected_watermark,
        )

    asyncio.run(run())

    assert len(sink.sent_transactions) == 1
    assert sink.sent_transactions[0][0] == (
        "bsc.raw_trace",
        [{"type": "trace", "block_number": 100, "trace_id": "100-root"}],
    )
    checkpoint_topic, checkpoint_rows = sink.sent_transactions[0][1]
    assert checkpoint_topic == "bsc.commit_watermark"
    assert checkpoint_rows[0]["cursor"] == 100


def test_engine_shutdown_waits_for_checkpoint_tasks_before_closing_sink():
    sink = ShutdownTrackingSink()
    engine = build_success_engine(sink=sink, eos_enabled=False)
    engine.watermark_manager = SimpleNamespace(
        start=lambda: asyncio.sleep(0),
        mark_emitted=lambda cursor: asyncio.sleep(0, result=cursor),
        requires_cursor_state=lambda cursor: asyncio.sleep(0, result=True),
        mark_completed=lambda cursor: asyncio.sleep(0, result=cursor),
        stop=lambda status="running": asyncio.sleep(0),
    )

    async def finalize_checkpoint(*_args, **_kwargs):
        await asyncio.sleep(0.05)
        sink.finalize_done.set()

    engine._finalize_checkpoint = finalize_checkpoint

    class OneShotCursorSource:
        def __init__(self):
            self.emitted = False

        async def next_cursor(self):
            if self.emitted:
                return None
            self.emitted = True
            return 100

    async def run():
        await engine.run_stream(OneShotCursorSource())

    asyncio.run(asyncio.wait_for(run(), timeout=1.0))

    assert sink.closed is True
    assert sink.close_started.is_set() is True


def test_run_stream_updates_producer_and_worker_heartbeats():
    """Live incident: rpcstream-log's producer()/worker() loops went
    genuinely idle for 5+ minutes with no crash, no error, and no
    ingestion_paused log -- two py-spy dumps minutes apart were identical
    and the exact stuck coroutine couldn't be pinpointed. These timestamps
    (exported as gauges, see EngineMetrics) turn a silent stall into a
    `time() - heartbeat` staleness value a dashboard can show without
    needing a debug pod + py-spy to even notice."""
    sink = RecordingSink()
    engine = build_success_engine(sink=sink, eos_enabled=False)
    assert engine._producer_heartbeat_ts == 0.0
    assert engine._worker_heartbeat_ts == 0.0

    class OneShotCursorSource:
        def __init__(self):
            self.emitted = False

        async def next_cursor(self):
            if self.emitted:
                return None
            self.emitted = True
            return 100

    before = time.time()
    asyncio.run(asyncio.wait_for(engine.run_stream(OneShotCursorSource()), timeout=1.0))
    after = time.time()

    assert before <= engine._producer_heartbeat_ts <= after
    assert before <= engine._worker_heartbeat_ts <= after
