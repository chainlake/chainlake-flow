import asyncio
from types import SimpleNamespace

from rpcstream.state.checkpoint import (
    CheckpointIdentity,
    KafkaCheckpointReader,
    WatermarkManager,
    build_checkpoint_identity,
)


class MemoryStore:
    def __init__(self):
        self.writes = []

    async def send_checkpoint(self, topic, row, wait_delivery=True):
        self.writes.append((topic, row, wait_delivery))


class FakeStateReader:
    def __init__(self, topic: str, records: dict | None = None):
        self.topic = topic
        self.records = records or {}

    def load(self):
        return self.records


def test_watermark_manager_advances_only_contiguous_completed_cursors():
    async def run():
        sink = MemoryStore()
        identity = CheckpointIdentity(
            pipeline="pipe",
            chain_uid="evm:56",
            chain_type="evm",
            network="mainnet",
            mode="realtime",
            primary_unit="block",
            entities=("block",),
        )
        manager = WatermarkManager(
            sink=sink,
            topic="checkpoint-topic",
            state_topic="watermark-state-topic",
            identity=identity,
            initial_cursor=99,
            flush_interval_ms=10000,
            commit_batch_size=100,
        )

        await manager.mark_emitted(100)
        await manager.mark_emitted(101)
        await manager.mark_emitted(102)
        await manager.mark_completed(100)
        await manager.mark_completed(102)
        assert manager.cursor == 100

        await manager.mark_completed(101)
        assert manager.cursor == 102

        await manager.stop(status="eos")
        return sink.writes

    writes = asyncio.run(run())
    assert len(writes) == 1
    topic, row, wait_delivery = writes[0]
    assert topic == "checkpoint-topic"
    assert wait_delivery is True
    assert row["cursor"] == 102
    assert row["status"] == "eos"
    assert row["id"].startswith("pipeline=pipe|")


def test_watermark_manager_waits_for_first_emitted_cursor_before_advancing():
    async def run():
        sink = MemoryStore()
        identity = CheckpointIdentity(
            pipeline="pipe",
            chain_uid="evm:56",
            chain_type="evm",
            network="mainnet",
            mode="realtime",
            primary_unit="block",
            entities=("block",),
        )
        manager = WatermarkManager(
            sink=sink,
            topic="checkpoint-topic",
            state_topic="watermark-state-topic",
            identity=identity,
            flush_interval_ms=10000,
        )

        await manager.mark_completed(101)
        assert manager.cursor is None

        await manager.mark_emitted(100)
        assert manager.cursor is None

        await manager.mark_completed(100)
        assert manager.cursor == 101

    asyncio.run(run())


def test_watermark_manager_preview_does_not_jump_failed_gap():
    async def run():
        sink = MemoryStore()
        identity = CheckpointIdentity(
            pipeline="pipe",
            chain_uid="evm:56",
            chain_type="evm",
            network="mainnet",
            mode="realtime",
            primary_unit="block",
            entities=("block",),
        )
        manager = WatermarkManager(
            sink=sink,
            topic="checkpoint-topic",
            state_topic="watermark-state-topic",
            identity=identity,
            initial_cursor=99,
            flush_interval_ms=10000,
            flush_on_advance=False,
        )

        await manager.mark_emitted(100)
        await manager.mark_emitted(101)

        assert await manager.preview_completed(101) is None
        assert await manager.mark_completed(101) is None
        assert manager.cursor == 99

        assert await manager.preview_completed(100) == 101
        assert await manager.mark_completed(100) == 101
        assert manager.cursor == 101

    asyncio.run(run())


def test_build_checkpoint_identity_uses_multichain_key_fields():
    runtime = SimpleNamespace(
        pipeline=SimpleNamespace(name="pipe", mode="backfill"),
        chain=SimpleNamespace(uid="sui:mainnet", type="sui", network="mainnet"),
        entities=["transaction", "checkpoint"],
    )

    identity = build_checkpoint_identity(runtime)

    assert identity.primary_unit == "checkpoint"
    assert "pipeline=pipe" in identity.key
    assert "chain=sui:mainnet" in identity.key
    assert "entities=checkpoint,transaction" in identity.key


def test_watermark_manager_merges_external_state_records():
    async def run():
        sink = MemoryStore()
        identity = CheckpointIdentity(
            pipeline="pipe",
            chain_uid="evm:56",
            chain_type="evm",
            network="mainnet",
            mode="realtime",
            primary_unit="block",
            entities=("block",),
        )
        state_reader = FakeStateReader("watermark-state-topic")
        manager = WatermarkManager(
            sink=sink,
            topic="checkpoint-topic",
            state_topic="watermark-state-topic",
            identity=identity,
            initial_cursor=99,
            flush_on_advance=False,
        )

        await manager.mark_emitted(100)
        await manager.mark_emitted(101)
        await manager.mark_completed(101)
        assert manager.cursor == 99

        state_reader.records = {
            100: SimpleNamespace(
                cursor=100,
                status="completed",
                updated_at_ms=1,
            )
        }
        await manager.merge_external_state_records(state_reader.load())

        assert manager.cursor == 101

    asyncio.run(run())


def test_watermark_manager_metrics_snapshot_tracks_cursor_gaps_and_delay():
    async def run():
        sink = MemoryStore()
        identity = CheckpointIdentity(
            pipeline="pipe",
            chain_uid="evm:56",
            chain_type="evm",
            network="mainnet",
            mode="realtime",
            primary_unit="block",
            entities=("block",),
        )
        manager = WatermarkManager(
            sink=sink,
            topic="checkpoint-topic",
            state_topic="watermark-state-topic",
            identity=identity,
            initial_cursor=99,
            flush_on_advance=False,
        )

        await manager.mark_emitted(100)
        await manager.mark_emitted(101)
        await manager.mark_failed(100, "boom")
        await manager.mark_completed(101)
        manager.update_commit_delay(7)

        return manager.get_metrics_snapshot()

    snapshot = asyncio.run(run())
    assert snapshot["commit_cursor"] == 99
    assert snapshot["gap_count"] == 1
    assert snapshot["oldest_gap"] == 100
    assert snapshot["commit_delay"] == 7


def test_kafka_checkpoint_reader_consumer_config_enables_partition_eof():
    identity = CheckpointIdentity(
        pipeline="pipe",
        chain_uid="evm:56",
        chain_type="evm",
        network="mainnet",
        mode="realtime",
        primary_unit="block",
        entities=("block",),
    )
    store = KafkaCheckpointReader(
        topic="evm.bsc.mainnet.commit_watermark",
        producer_config={"bootstrap.servers": "localhost:9092", "linger.ms": 50},
        identity=identity,
    )

    config = store._consumer_config()

    assert config["enable.partition.eof"] is True
    assert config["isolation.level"] == "read_committed"
    assert config["auto.offset.reset"] == "earliest"
    assert "linger.ms" not in config
