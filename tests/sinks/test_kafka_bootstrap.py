import asyncio
from types import SimpleNamespace

from rpcstream.sinks.kafka.bootstrap import build_protobuf_topic_schemas
from rpcstream.sinks.kafka.producer import KafkaWriter


def test_build_protobuf_topic_schemas_includes_main_and_dlq_topics():
    topic_maps = SimpleNamespace(
        main={
            "block": "evm.bsc.mainnet.raw_block",
            "trace": "evm.bsc.mainnet.raw_trace",
        },
        dlq="dlq.ingestion",
    )

    schemas = build_protobuf_topic_schemas(topic_maps, ["block", "trace"])

    assert set(schemas) == {
        "evm.bsc.mainnet.raw_block",
        "evm.bsc.mainnet.raw_trace",
        "dlq.ingestion",
    }


def test_kafka_writer_start_runs_protobuf_warmup():
    class DummyProducer:
        def poll(self, _timeout):
            return None

        def flush(self):
            return None

    class WarmupRegistry:
        def __init__(self):
            self.started = False
            self.schema_registry_url = "https://registry.example.com"
            self.topic_schemas = {"topic-a": object(), "dlq.ingestion": object()}

        def start(self):
            self.started = True

    writer = KafkaWriter(
        producer=DummyProducer(),
        id_calculator=SimpleNamespace(calculate_event_id=lambda row: "evt-1"),
        time_calculator=SimpleNamespace(calculate_ingest_timestamp=lambda: 1),
        logger=None,
        config=SimpleNamespace(batch_size=10, flush_interval_ms=10, queue_maxsize=10),
        producer_config={"bootstrap.servers": "localhost:9092"},
        topic_maps=SimpleNamespace(main={"block": "topic-a"}, dlq="dlq.ingestion"),
        protobuf_enabled=False,
    )
    writer.protobuf_registry = WarmupRegistry()

    async def run():
        await writer.start()
        await writer.close()

    asyncio.run(run())
    assert writer.protobuf_registry.started is True


def test_kafka_writer_serializes_protobuf_lazily():
    class DummyProducer:
        pass

    class LazyRegistry:
        def __init__(self):
            self.serialized = []

        def serialize(self, topic, row):
            self.serialized.append((topic, row.copy()))
            return b"protobuf-payload"

    writer = KafkaWriter(
        producer=DummyProducer(),
        id_calculator=SimpleNamespace(calculate_event_id=lambda row: "evt-1"),
        time_calculator=SimpleNamespace(calculate_ingest_timestamp=lambda: 1),
        logger=None,
        config=SimpleNamespace(batch_size=10, flush_interval_ms=10, queue_maxsize=10),
        producer_config={"bootstrap.servers": "localhost:9092"},
        topic_maps=SimpleNamespace(main={"block": "topic-a"}, dlq="dlq.ingestion"),
        protobuf_enabled=False,
    )
    writer.protobuf_registry = LazyRegistry()

    payload = writer._serialize("topic-a", {"id": "evt-1"})

    assert payload == b"protobuf-payload"
    assert writer.protobuf_registry.serialized == [("topic-a", {"id": "evt-1"})]


def test_kafka_writer_wait_delivery_future_resolves_after_callback():
    class Message:
        def topic(self):
            return "topic-a"

        def partition(self):
            return 0

        def offset(self):
            return 1

    class DummyProducer:
        def __init__(self):
            self.callbacks = []

        def produce(self, **kwargs):
            self.callbacks.append(kwargs["callback"])

        def poll(self, _timeout):
            while self.callbacks:
                self.callbacks.pop(0)(None, Message())

        def flush(self):
            self.poll(0)

    producer = DummyProducer()
    writer = KafkaWriter(
        producer=producer,
        id_calculator=SimpleNamespace(calculate_event_id=lambda row: row["id"]),
        time_calculator=SimpleNamespace(calculate_ingest_timestamp=lambda: 1),
        logger=None,
        config=SimpleNamespace(batch_size=10, flush_interval_ms=1, queue_maxsize=10),
        producer_config={"bootstrap.servers": "localhost:9092"},
        topic_maps=SimpleNamespace(main={"block": "topic-a"}, dlq="dlq.ingestion"),
        protobuf_enabled=False,
    )

    async def run():
        await writer.start()
        future = await writer.send("topic-a", [{"id": "evt-1"}], wait_delivery=True)
        await writer.close()
        result = await asyncio.wait_for(future, timeout=1)
        return future.done(), result

    assert asyncio.run(run()) == (True, True)


def test_kafka_writer_send_transaction_commits_business_and_checkpoint():
    class DummyProducer:
        def __init__(self):
            self.events = []

        def init_transactions(self, timeout=None):
            self.events.append(("init", timeout))

        def begin_transaction(self):
            self.events.append(("begin",))

        def produce(self, topic, key, value, callback=None):
            self.events.append(("produce", topic, key, value))
            if callback:
                callback(None, SimpleNamespace(topic=lambda: topic, partition=lambda: 0, offset=lambda: 1))

        def poll(self, _timeout):
            return None

        def commit_transaction(self):
            self.events.append(("commit",))

        def abort_transaction(self):
            self.events.append(("abort",))

        def flush(self):
            return None

    producer = DummyProducer()
    writer = KafkaWriter(
        producer=producer,
        id_calculator=SimpleNamespace(calculate_event_id=lambda row: row["id"]),
        time_calculator=SimpleNamespace(calculate_ingest_timestamp=lambda: 1),
        logger=None,
        config=SimpleNamespace(batch_size=10, flush_interval_ms=1, queue_maxsize=10),
        producer_config={
            "bootstrap.servers": "localhost:9092",
            "transactional.id": "tx-1",
        },
        topic_maps=SimpleNamespace(main={"block": "topic-a"}, dlq="dlq.ingestion"),
        protobuf_enabled=False,
        eos_enabled=True,
        eos_init_timeout_sec=12,
    )

    async def run():
        await writer.start()
        await writer.send_transaction(
            [("topic-a", [{"id": "evt-1", "type": "block"}])],
            "checkpoint-topic",
            "checkpoint-key",
            b'{"cursor":1}',
        )
        await writer.close()

    asyncio.run(run())

    assert ("init", 12) in producer.events
    assert producer.events[1] == ("begin",)
    assert ("produce", "topic-a", "evt-1", '{"id":"evt-1","type":"block","ingest_timestamp":1}') in producer.events
    assert ("produce", "checkpoint-topic", "checkpoint-key", b'{"cursor":1}') in producer.events
    assert ("commit",) in producer.events
    assert ("abort",) not in producer.events
