import asyncio
from types import SimpleNamespace

from rpcstream.state.checkpoint import (
    CheckpointIdentity,
    CheckpointManager,
    KafkaCheckpointStore,
    build_checkpoint_identity,
)


class MemoryStore:
    def __init__(self):
        self.writes = []

    async def write(self, cursor, status="running", error=None):
        self.writes.append((cursor, status, error))


def test_checkpoint_manager_advances_only_contiguous_completed_cursors():
    async def run():
        store = MemoryStore()
        manager = CheckpointManager(
            store=store,
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
        return store.writes

    assert asyncio.run(run()) == [(102, "eos", None)]


def test_checkpoint_manager_waits_for_first_emitted_cursor_before_advancing():
    async def run():
        store = MemoryStore()
        manager = CheckpointManager(store=store, flush_interval_ms=10000)

        await manager.mark_completed(101)
        assert manager.cursor is None

        await manager.mark_emitted(100)
        assert manager.cursor is None

        await manager.mark_completed(100)
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


def test_kafka_checkpoint_store_consumer_config_enables_partition_eof():
    identity = CheckpointIdentity(
        pipeline="pipe",
        chain_uid="evm:56",
        chain_type="evm",
        network="mainnet",
        mode="realtime",
        primary_unit="block",
        entities=("block",),
    )
    store = KafkaCheckpointStore(
        topic="evm.bsc.mainnet.checkpoint_cursor",
        producer_config={"bootstrap.servers": "localhost:9092", "linger.ms": 50},
        identity=identity,
    )

    config = store._consumer_config()

    assert config["enable.partition.eof"] is True
    assert config["isolation.level"] == "read_committed"
    assert config["auto.offset.reset"] == "earliest"
    assert "linger.ms" not in config
