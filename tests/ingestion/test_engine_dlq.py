import asyncio
from types import SimpleNamespace

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


def build_engine(*, sink, eos_enabled=False):
    return IngestionEngine(
        fetcher=DummyFetcher(value=[]),
        processors={"trace": FailingTraceProcessor()},
        enricher=EvmEnricher(),
        sink=sink,
        topics={"trace": "evm.bsc.mainnet.raw_trace"},
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
        topics={"trace": "evm.bsc.mainnet.raw_trace"},
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
        topics={"trace": "evm.bsc.mainnet.raw_trace"},
        dlq_topic="dlq.ingestion",
        chain=SimpleNamespace(type="evm", network_label="bsc-mainnet"),
        pipeline=SimpleNamespace(
            name="bsc_mainnet_backfill_10_20",
            mode="backfill",
            end_block=20,
        ),
        max_retry=1,
        concurrency=1,
        logger=None,
        watermark_manager=None,
        checkpoint_reader=None,
        eos_enabled=False,
    )


def test_engine_sends_trace_dlq_record_when_processor_fails():
    sink = RecordingSink()
    engine = build_engine(sink=sink, eos_enabled=False)

    success, delivery_futures, expected_watermark = asyncio.run(engine._run_one(95281318))

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
    assert record["block_number"] == 95281318
    assert record["stage"] == "processor"
    assert record["error_type"] == "TypeError"
    assert record["error_message"] == "list indices must be integers or slices, not str"
    assert record["status"] == "pending"


def test_engine_sends_trace_dlq_via_transaction_when_eos_enabled():
    sink = RecordingSink()
    engine = build_engine(sink=sink, eos_enabled=True)

    success, delivery_futures, expected_watermark = asyncio.run(engine._run_one(95281318))

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

    success, delivery_futures, expected_watermark = asyncio.run(engine._run_one(95281318))

    assert success is True
    assert delivery_futures == []
    assert expected_watermark is None
    assert sink.sent == []
    assert len(sink.sent_transactions) == 1
    topic_rows = sink.sent_transactions[0]
    assert topic_rows == [
        (
            "evm.bsc.mainnet.raw_trace",
            [{"type": "trace", "block_number": 95281318, "trace_id": "95281318-root"}],
        )
    ]


def test_engine_marks_dlq_resolved_via_transaction_when_eos_enabled():
    sink = RecordingSink()
    engine = build_engine(sink=sink, eos_enabled=True)

    record = {
        "id": "dlq-1",
        "chain": "evm",
        "network": "bsc-mainnet",
        "pipeline": "bsc_mainnet_realtime_checkpoint",
        "entity": "trace",
        "block_number": 95281318,
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
        topic="evm.bsc.mainnet.commit_watermark",
        state_topic="evm.bsc.mainnet.cursor_state",
        identity=identity,
        initial_cursor=99,
        flush_on_advance=False,
    )
    engine = build_success_engine(sink=sink, eos_enabled=True)
    engine.watermark_manager = watermark_manager

    async def run():
        await watermark_manager.mark_emitted(100)
        await watermark_manager.mark_emitted(101)

        success_101, delivery_futures_101, expected_101 = await engine._run_one(101)
        await engine._finalize_checkpoint(
            101,
            success_101,
            delivery_futures_101,
            expected_watermark=expected_101,
        )

        success_100, delivery_futures_100, expected_100 = await engine._run_one(100)
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
        "evm.bsc.mainnet.raw_trace",
        [{"type": "trace", "block_number": 101, "trace_id": "101-root"}],
    )
    state_topic_101, state_rows_101 = sink.sent_transactions[0][1]
    assert state_topic_101 == "evm.bsc.mainnet.cursor_state"
    assert len(state_rows_101) == 1
    assert state_rows_101[0]["cursor"] == 101
    assert state_rows_101[0]["status"] == "completed"
    assert sink.sent_transactions[1][0] == (
        "evm.bsc.mainnet.raw_trace",
        [{"type": "trace", "block_number": 100, "trace_id": "100-root"}],
    )
    checkpoint_topic, checkpoint_rows = sink.sent_transactions[1][1]
    assert checkpoint_topic == "evm.bsc.mainnet.commit_watermark"
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
        topic="evm.bsc.mainnet.commit_watermark",
        state_topic="evm.bsc.mainnet.cursor_state",
        identity=identity,
        initial_cursor=99,
        flush_on_advance=False,
    )
    engine = build_success_engine(sink=sink, eos_enabled=True)
    engine.watermark_manager = watermark_manager

    async def run():
        await watermark_manager.mark_emitted(100)
        success, delivery_futures, expected_watermark = await engine._run_one(100)
        await engine._finalize_checkpoint(
            100,
            success,
            delivery_futures,
            expected_watermark=expected_watermark,
        )

    asyncio.run(run())

    assert len(sink.sent_transactions) == 1
    assert sink.sent_transactions[0][0] == (
        "evm.bsc.mainnet.raw_trace",
        [{"type": "trace", "block_number": 100, "trace_id": "100-root"}],
    )
    checkpoint_topic, checkpoint_rows = sink.sent_transactions[0][1]
    assert checkpoint_topic == "evm.bsc.mainnet.commit_watermark"
    assert checkpoint_rows[0]["cursor"] == 100
