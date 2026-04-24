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


def test_kafka_writer_start_runs_only_protobuf_warmup():
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
