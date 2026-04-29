import asyncio
from types import SimpleNamespace

from rpcstream.ingestion.engine import IngestionEngine


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

    async def send_transaction(self, topic_rows, checkpoint_topic, checkpoint_key, checkpoint_value):
        self.sent_transactions.append(
            (topic_rows, checkpoint_topic, checkpoint_key, checkpoint_value)
        )


def build_engine(*, sink, eos_enabled=False):
    return IngestionEngine(
        fetcher=DummyFetcher(value=[]),
        processors={"trace": FailingTraceProcessor()},
        sink=sink,
        topics={"trace": "evm.bsc.mainnet.raw_trace"},
        dlq_topic="dlq.ingestion",
        chain=SimpleNamespace(type="evm", network_label="bsc-mainnet"),
        pipeline=SimpleNamespace(name="bsc_mainnet_realtime_checkpointed_latest"),
        max_retry=1,
        concurrency=1,
        logger=None,
        checkpoint_manager=None,
        checkpoint_store=None,
        eos_enabled=eos_enabled,
    )


def build_success_engine(*, sink, eos_enabled=False):
    return IngestionEngine(
        fetcher=DummyFetcher(value=[]),
        processors={"trace": SuccessfulTraceProcessor()},
        sink=sink,
        topics={"trace": "evm.bsc.mainnet.raw_trace"},
        dlq_topic="dlq.ingestion",
        chain=SimpleNamespace(type="evm", network_label="bsc-mainnet"),
        pipeline=SimpleNamespace(name="bsc_mainnet_realtime_checkpointed_latest"),
        max_retry=1,
        concurrency=1,
        logger=None,
        checkpoint_manager=None,
        checkpoint_store=None,
        eos_enabled=eos_enabled,
    )


def test_engine_sends_trace_dlq_record_when_processor_fails():
    sink = RecordingSink()
    engine = build_engine(sink=sink, eos_enabled=False)

    success, delivery_futures = asyncio.run(engine._run_one(95281318))

    assert success is False
    assert delivery_futures == []
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

    success, delivery_futures = asyncio.run(engine._run_one(95281318))

    assert success is False
    assert delivery_futures == []
    assert sink.sent == []
    assert len(sink.sent_transactions) == 1
    topic_rows, checkpoint_topic, checkpoint_key, checkpoint_value = sink.sent_transactions[0]
    assert checkpoint_topic is None
    assert checkpoint_key is None
    assert checkpoint_value is None
    assert len(topic_rows) == 1
    topic, rows = topic_rows[0]
    assert topic == "dlq.ingestion"
    assert len(rows) == 1
    assert rows[0]["entity"] == "trace"


def test_engine_sends_business_rows_via_transaction_when_eos_enabled_without_checkpoint():
    sink = RecordingSink()
    engine = build_success_engine(sink=sink, eos_enabled=True)

    success, delivery_futures = asyncio.run(engine._run_one(95281318))

    assert success is True
    assert delivery_futures == []
    assert sink.sent == []
    assert len(sink.sent_transactions) == 1
    topic_rows, checkpoint_topic, checkpoint_key, checkpoint_value = sink.sent_transactions[0]
    assert checkpoint_topic is None
    assert checkpoint_key is None
    assert checkpoint_value is None
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
        "pipeline": "bsc_mainnet_realtime_checkpointed_latest",
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
    topic_rows, checkpoint_topic, checkpoint_key, checkpoint_value = sink.sent_transactions[0]
    assert checkpoint_topic is None
    assert checkpoint_key is None
    assert checkpoint_value is None
    assert len(topic_rows) == 1
    assert topic_rows[0][0] == "dlq.ingestion"
    assert topic_rows[0][1][0]["status"] == "resolved"
