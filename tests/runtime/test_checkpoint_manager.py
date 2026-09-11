import asyncio
from types import SimpleNamespace

from rpcstream.state.checkpoint import (
    CheckpointIdentity,
    KafkaCheckpointReader,
    KafkaWatermarkStateReader,
    WatermarkManager,
    WatermarkStateRecord,
    build_checkpoint_identity,
    build_watermark_state_key,
    build_watermark_state_row,
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

        await manager.stop(status="completed")
        return sink.writes

    writes = asyncio.run(run())
    assert len(writes) == 1
    topic, row, wait_delivery = writes[0]
    assert topic == "checkpoint-topic"
    assert wait_delivery is True
    assert row["cursor"] == 102
    assert row["status"] == "completed"
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
    assert "entities=checkpoint,transaction" in identity.key


def test_watermark_state_key_is_shortened():
    identity = CheckpointIdentity(
        pipeline="bsc_mainnet_backfill_96098686_96098785",
        chain_uid="evm:56",
        chain_type="evm",
        network="mainnet",
        mode="backfill",
        primary_unit="block",
        entities=("block", "log", "token_transfer", "transaction"),
    )

    key = build_watermark_state_key(identity, 96098738)
    row = build_watermark_state_row(identity, 96098738, status="completed")

    assert key == (
        "pipeline=bsc_mainnet_backfill_96098686_96098785|"
        "entities=block,log,token_transfer,transaction|cursor=96098738"
    )
    assert row["id"] == key
    assert row["kafka_partition_key"] == key
    assert "chain=evm:56" not in key
    assert "network=mainnet" not in key
    assert "mode=backfill" not in key
    assert "unit=block" not in key


def test_checkpoint_identity_key_is_shortened():
    identity = CheckpointIdentity(
        pipeline="bsc_mainnet_backfill_96098686_96098785",
        chain_uid="evm:56",
        chain_type="evm",
        network="mainnet",
        mode="backfill",
        primary_unit="block",
        entities=("block", "log", "token_transfer", "transaction"),
    )

    assert identity.key == (
        "pipeline=bsc_mainnet_backfill_96098686_96098785|"
        "entities=block,log,token_transfer,transaction"
    )
    assert "chain=evm:56" not in identity.key
    assert "network=mainnet" not in identity.key
    assert "mode=backfill" not in identity.key
    assert "unit=block" not in identity.key


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


def test_watermark_manager_mark_failed_increments_cursor_failed_counter():
    """rpcstream_watermark_gap_count is a point-in-time gauge (current size
    of the unresolved-cursor set) -- it nets new failures against retries
    resolving old ones, so it can sit flat while watermark.cursor_failed
    events are actively firing underneath it. rpcstream_watermark_cursor_
    failed_total is the missing rate-of-new-failures signal, needed to
    show this event on a Grafana dashboard at all (previously log-only)."""
    from opentelemetry.sdk.metrics import MeterProvider as SDKMeterProvider
    from opentelemetry.sdk.metrics.export import InMemoryMetricReader

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
        reader = InMemoryMetricReader()
        provider = SDKMeterProvider(metric_readers=[reader])
        meter = provider.get_meter("rpcstream.watermark")

        manager = WatermarkManager(
            sink=sink,
            topic="checkpoint-topic",
            state_topic="watermark-state-topic",
            identity=identity,
            initial_cursor=99,
            flush_on_advance=False,
            meter=meter,
        )

        await manager.mark_failed(100, "boom")
        await manager.mark_failed(101, "boom again")

        reader.collect()
        data = reader.get_metrics_data()
        for rm in data.resource_metrics:
            for sm in rm.scope_metrics:
                for m_obj in sm.metrics:
                    if m_obj.name == "rpcstream_watermark_cursor_failed_total":
                        return sum(dp.value for dp in m_obj.data.data_points)
        return None

    total = asyncio.run(run())
    assert total == 2


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
        topic="bsc.commit_watermark",
        producer_config={"bootstrap.servers": "localhost:9092", "linger.ms": 50},
        identity=identity,
    )

    config = store._consumer_config()

    assert config["enable.partition.eof"] is True
    assert config["isolation.level"] == "read_committed"
    assert config["auto.offset.reset"] == "earliest"
    assert "linger.ms" not in config


def test_kafka_checkpoint_reader_returns_none_when_schema_is_missing(monkeypatch):
    class FakeMessage:
        def error(self):
            return None

        def partition(self):
            return 0

        def offset(self):
            return 0

        def key(self):
            return b"pipeline=pipe|entities=block"

        def value(self):
            return b"payload"

    class FakeTopicMeta:
        error = None

        def __init__(self):
            self.partitions = {0: object()}

    class FakeMetadata:
        def __init__(self):
            self.topics = {"bsc.commit_watermark": FakeTopicMeta()}

    class FakeConsumer:
        def __init__(self, *_args, **_kwargs):
            self._polled = False

        def list_topics(self, *_args, **_kwargs):
            return FakeMetadata()

        def get_watermark_offsets(self, *_args, **_kwargs):
            return (0, 1)

        def assign(self, *_args, **_kwargs):
            return None

        def consume(self, num_messages=500, timeout=1.0):
            messages = []
            for _ in range(num_messages):
                message = self.poll()
                if message is None:
                    break
                messages.append(message)
            return messages

        def poll(self, *_args, **_kwargs):
            if self._polled:
                return None
            self._polled = True
            return FakeMessage()

        def close(self):
            return None

    monkeypatch.setattr("confluent_kafka.Consumer", FakeConsumer)

    identity = CheckpointIdentity(
        pipeline="pipe",
        chain_uid="evm:56",
        chain_type="evm",
        network="mainnet",
        mode="realtime",
        primary_unit="block",
        entities=("block",),
    )
    reader = KafkaCheckpointReader(
        topic="bsc.commit_watermark",
        producer_config={"bootstrap.servers": "localhost:9092"},
        identity=identity,
        schema_registry_url="http://localhost:30081",
    )
    monkeypatch.setattr(
        reader,
        "_decode_record",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            RuntimeError("Schema 12 not found (HTTP status code 404, SR code 40403)")
        ),
    )

    assert reader.load() is None
    assert reader.schema_missing is True


def test_kafka_watermark_state_reader_returns_empty_when_schema_is_missing(monkeypatch):
    class FakeMessage:
        def error(self):
            return None

        def partition(self):
            return 0

        def offset(self):
            return 0

        def key(self):
            return b"pipeline=pipe|entities=block|cursor=1"

        def value(self):
            return b"payload"

    class FakeTopicMeta:
        error = None

        def __init__(self):
            self.partitions = {0: object()}

    class FakeMetadata:
        def __init__(self):
            self.topics = {"bsc.cursor_state": FakeTopicMeta()}

    class FakeConsumer:
        def __init__(self, *_args, **_kwargs):
            self._polled = False

        def list_topics(self, *_args, **_kwargs):
            return FakeMetadata()

        def get_watermark_offsets(self, *_args, **_kwargs):
            return (0, 1)

        def assign(self, *_args, **_kwargs):
            return None

        def position(self, partitions):
            # -1001 == confluent_kafka.OFFSET_INVALID: no fetch has happened
            # on this partition yet (this reader's first-ever call).
            return [SimpleNamespace(offset=-1001) for _ in partitions]

        def consume(self, num_messages=500, timeout=1.0):
            messages = []
            for _ in range(num_messages):
                message = self.poll()
                if message is None:
                    break
                messages.append(message)
            return messages

        def poll(self, *_args, **_kwargs):
            if self._polled:
                return None
            self._polled = True
            return FakeMessage()

        def close(self):
            return None

    monkeypatch.setattr("confluent_kafka.Consumer", FakeConsumer)

    identity = CheckpointIdentity(
        pipeline="pipe",
        chain_uid="evm:56",
        chain_type="evm",
        network="mainnet",
        mode="realtime",
        primary_unit="block",
        entities=("block",),
    )
    reader = KafkaWatermarkStateReader(
        topic="bsc.cursor_state",
        producer_config={"bootstrap.servers": "localhost:9092"},
        identity=identity,
        schema_registry_url="http://localhost:30081",
    )
    monkeypatch.setattr(
        reader,
        "_decode_record",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            RuntimeError("Schema 12 not found (HTTP status code 404, SR code 40403)")
        ),
    )

    assert reader.load() == {}
    assert reader.schema_missing is True


def test_kafka_watermark_state_reader_load_is_incremental(monkeypatch):
    """load() used to create a brand-new Consumer and re-scan the whole
    topic from `auto.offset.reset: earliest` on every call -- with
    `enable.auto.commit: False` and no seek, there was no persisted
    position to resume from, so every refresh (roughly once a second,
    forever) re-read and re-decoded the entire history. Live, this topic
    held ~207k raw messages for ~13k distinct cursor keys (it's compacted,
    but compaction only runs over closed segments and lags real time), so
    every refresh was re-decoding ~15x more records than useful, took 30+
    seconds, and kept getting slower as the topic grew -- the actual driver
    behind rpcstream falling behind, not Kafka message serialization. The
    reader must keep its Consumer alive across calls and only poll for
    messages appended since the previous call, merging them into what it
    already knows instead of rebuilding from scratch."""
    import json

    class FakeMessage:
        def __init__(self, partition, offset, key, value):
            self._partition = partition
            self._offset = offset
            self._key = key
            self._value = value

        def error(self):
            return None

        def partition(self):
            return self._partition

        def offset(self):
            return self._offset

        def key(self):
            return self._key

        def value(self):
            return self._value

    class FakeTopicMeta:
        error = None

        def __init__(self):
            self.partitions = {0: object()}

    class FakeMetadata:
        def __init__(self):
            self.topics = {"bsc.cursor_state": FakeTopicMeta()}

    topic_log: list = []  # shared "broker" state: append-only
    construction_count = 0

    class FakeConsumer:
        def __init__(self, *_args, **_kwargs):
            nonlocal construction_count
            construction_count += 1
            self._next_offset = 0

        def list_topics(self, *_args, **_kwargs):
            return FakeMetadata()

        def get_watermark_offsets(self, *_args, **_kwargs):
            return (0, len(topic_log))

        def assign(self, *_args, **_kwargs):
            return None

        def position(self, partitions):
            offset = -1001 if self._next_offset == 0 else self._next_offset
            return [SimpleNamespace(offset=offset) for _ in partitions]

        def poll(self, *_args, **_kwargs):
            if self._next_offset >= len(topic_log):
                return None
            message = topic_log[self._next_offset]
            self._next_offset += 1
            return message

        def consume(self, num_messages=500, timeout=1.0):
            messages = []
            for _ in range(num_messages):
                message = self.poll()
                if message is None:
                    break
                messages.append(message)
            return messages

        def close(self):
            return None

    monkeypatch.setattr("confluent_kafka.Consumer", FakeConsumer)

    identity = CheckpointIdentity(
        pipeline="pipe",
        chain_uid="evm:56",
        chain_type="evm",
        network="mainnet",
        mode="realtime",
        primary_unit="block",
        entities=("block",),
    )
    reader = KafkaWatermarkStateReader(
        topic="bsc.cursor_state",
        producer_config={"bootstrap.servers": "localhost:9092"},
        identity=identity,
        schema_registry_url=None,  # falls back to plain JSON decode
    )

    topic_log.append(
        FakeMessage(
            0, 0, b"pipeline=pipe|entities=block|cursor=1",
            json.dumps({"cursor": 1, "status": "failed"}).encode(),
        )
    )

    first = reader.load()
    assert set(first) == {1}
    assert construction_count == 1

    # A new record gets appended to the topic between refresh cycles.
    topic_log.append(
        FakeMessage(
            0, 1, b"pipeline=pipe|entities=block|cursor=2",
            json.dumps({"cursor": 2, "status": "completed"}).encode(),
        )
    )

    second = reader.load()
    # load() now returns only the delta — records newly read in this call.
    # Cursor 1 was already returned in `first`; cursor 2 is the only new one.
    # The caller (WatermarkManager._refresh_loop) accumulates state across
    # calls via merge_external_state_records, so it doesn't need the full
    # snapshot every tick — only the delta.
    assert set(second) == {2}
    # Still the same Consumer: no second construction, and the fake's
    # internal position only advances forward -- if load() had re-scanned
    # from the start, poll() would have to be called for offset 0 again,
    # which this fake can't do without resetting _next_offset itself.
    assert construction_count == 1


# ---------------------------------------------------------------------------
# Unresolved-gap policy: a single cursor that never succeeds must not be able
# to pin the contiguous watermark forever, because every advance-released
# structure (_completed, _state_versions, the persisted checkpoint) then grows
# without bound. Live incident: 41 permanent gaps -> ~112 MB/day RSS growth ->
# OOMKill after 3.3 days, then a restart loop that OOMKilled in ~86s.
# ---------------------------------------------------------------------------


def _identity() -> CheckpointIdentity:
    return CheckpointIdentity(
        pipeline="pipe",
        chain_uid="evm:56",
        chain_type="evm",
        network="mainnet",
        mode="realtime",
        primary_unit="block",
        entities=("block",),
    )


def test_gap_is_force_resolved_once_it_exceeds_max_gap_age():
    async def run():
        sink = MemoryStore()
        manager = WatermarkManager(
            sink=sink,
            topic="checkpoint-topic",
            state_topic="watermark-state-topic",
            identity=_identity(),
            initial_cursor=99,
            flush_interval_ms=10000,
            commit_batch_size=100,
            max_gap_age_sec=0.05,
            max_gap_count=0,
        )

        await manager.mark_failed(100, "sink timeout")
        # Not old enough yet: the gap must still hold the watermark back.
        assert manager.cursor == 99
        assert 100 in manager._failed

        await asyncio.sleep(0.06)
        # A second failure re-runs the bound; 100 is now past max_gap_age_sec.
        await manager.mark_failed(101, "sink timeout")

        assert manager.cursor == 100, "watermark must move past the expired gap"
        assert 100 not in manager._failed
        assert 101 in manager._failed, "the fresh gap must be kept"
        assert manager._completed == set(), "resolved gap must be consumed, not re-queued"

        await manager.stop(status="completed")

    asyncio.run(run())


def test_gap_count_bound_sheds_the_oldest_gaps():
    async def run():
        sink = MemoryStore()
        manager = WatermarkManager(
            sink=sink,
            topic="checkpoint-topic",
            state_topic="watermark-state-topic",
            identity=_identity(),
            initial_cursor=99,
            flush_interval_ms=10000,
            commit_batch_size=100,
            max_gap_age_sec=0,
            max_gap_count=2,
        )

        await manager.mark_failed(100, "boom")
        await manager.mark_failed(101, "boom")
        assert len(manager._failed) == 2
        assert manager.cursor == 99

        # Third failure pushes the set over budget -> oldest gap is shed.
        await manager.mark_failed(102, "boom")
        assert manager._failed == {101, 102}
        assert manager.cursor == 100

        await manager.stop(status="completed")

    asyncio.run(run())


def test_gap_policy_can_be_disabled():
    async def run():
        sink = MemoryStore()
        manager = WatermarkManager(
            sink=sink,
            topic="checkpoint-topic",
            state_topic="watermark-state-topic",
            identity=_identity(),
            initial_cursor=99,
            flush_interval_ms=10000,
            commit_batch_size=100,
            max_gap_age_sec=0,
            max_gap_count=0,
        )

        await manager.mark_failed(100, "boom")
        await manager.mark_failed(101, "boom")
        await manager.mark_emitted(102)
        await manager.mark_completed(102)

        # Both bounds off: the gap is never shed, so nothing above it commits.
        assert manager._failed == {100, 101}
        assert manager.cursor == 99

        await manager.stop(status="completed")

    asyncio.run(run())


def test_forced_gap_resolution_is_counted():
    from opentelemetry.sdk.metrics import MeterProvider as SDKMeterProvider
    from opentelemetry.sdk.metrics.export import InMemoryMetricReader

    async def run():
        sink = MemoryStore()
        reader = InMemoryMetricReader()
        provider = SDKMeterProvider(metric_readers=[reader])
        manager = WatermarkManager(
            sink=sink,
            topic="checkpoint-topic",
            state_topic="watermark-state-topic",
            identity=_identity(),
            initial_cursor=99,
            flush_interval_ms=10000,
            commit_batch_size=100,
            max_gap_age_sec=0,
            max_gap_count=1,
            meter=provider.get_meter("rpcstream.watermark"),
        )

        await manager.mark_failed(100, "boom")
        await manager.mark_failed(101, "boom")

        reader.collect()
        data = reader.get_metrics_data()
        for rm in data.resource_metrics:
            for sm in rm.scope_metrics:
                for metric in sm.metrics:
                    if metric.name == "rpcstream_watermark_gap_forced_resolved_total":
                        return sum(dp.value for dp in metric.data.data_points)
        return None

    assert asyncio.run(run()) == 1


def test_requires_cursor_state_respects_persist_window():
    async def run():
        sink = MemoryStore()
        manager = WatermarkManager(
            sink=sink,
            topic="checkpoint-topic",
            state_topic="watermark-state-topic",
            identity=_identity(),
            initial_cursor=99,
            flush_interval_ms=10000,
            commit_batch_size=100,
            max_gap_age_sec=0,
            max_gap_count=0,
            state_persist_window=5,
        )

        await manager.mark_emitted(100)
        await manager.mark_completed(100)
        assert manager.cursor == 100  # _next_cursor is now 101

        # Inside the window: the in-process _completed set already covers it,
        # so no state row (and later no tombstone) is written.
        assert await manager.requires_cursor_state(104) is False
        # Beyond the window: genuinely far ahead, persist it.
        assert await manager.requires_cursor_state(110) is True
        # A previously failed cursor is always persisted for visibility.
        await manager.mark_failed(108, "boom")
        assert await manager.requires_cursor_state(108) is True

        await manager.stop(status="completed")

    asyncio.run(run())


def test_zero_persist_window_keeps_legacy_behaviour():
    async def run():
        sink = MemoryStore()
        manager = WatermarkManager(
            sink=sink,
            topic="checkpoint-topic",
            state_topic="watermark-state-topic",
            identity=_identity(),
            initial_cursor=99,
            flush_interval_ms=10000,
            commit_batch_size=100,
            max_gap_age_sec=0,
            max_gap_count=0,
            state_persist_window=0,
        )

        await manager.mark_emitted(100)
        await manager.mark_completed(100)
        # cursor == 100, so _next_cursor is 101; anything above it is persisted
        # when the window is disabled (the historical behaviour).
        assert await manager.requires_cursor_state(101) is False
        assert await manager.requires_cursor_state(102) is True

        await manager.stop(status="completed")

    asyncio.run(run())


def test_hydrate_skips_committed_cursors_before_recording_versions():
    """A cold start against a large state topic must not park every
    already-committed cursor in _state_versions (observed 687,491 entries)."""

    def record(cursor, status="completed"):
        return WatermarkStateRecord(
            cursor=cursor,
            status=status,
            updated_at_ms=1,
            identity=_identity(),
        )

    manager = WatermarkManager(
        sink=MemoryStore(),
        topic="checkpoint-topic",
        state_topic="watermark-state-topic",
        identity=_identity(),
        initial_cursor=100,
        state_records={50: record(50), 100: record(100), 150: record(150)},
        flush_interval_ms=10000,
        commit_batch_size=100,
    )

    assert set(manager._state_versions) == {150}
    assert manager._completed == {150}
    assert manager.cursor == 100


def test_merge_external_state_skips_committed_cursors():
    async def run():
        sink = MemoryStore()
        manager = WatermarkManager(
            sink=sink,
            topic="checkpoint-topic",
            state_topic="watermark-state-topic",
            identity=_identity(),
            initial_cursor=100,
            flush_interval_ms=10000,
            commit_batch_size=100,
            max_gap_age_sec=0,
            max_gap_count=0,
        )

        def record(cursor):
            return WatermarkStateRecord(
                cursor=cursor,
                status="completed",
                updated_at_ms=1,
                identity=_identity(),
            )

        # 50 is at/below the commit watermark -> dropped without ever entering
        # the version map. 102 is left pending (101 never completed) so it
        # survives the post-advance prune and proves it *was* recorded.
        await manager.merge_external_state_records({50: record(50), 102: record(102)})

        assert set(manager._state_versions) == {102}
        assert manager.cursor == 100

        await manager.stop(status="completed")

    asyncio.run(run())


def test_pending_hole_is_shed_when_completed_set_exceeds_bound():
    """A cursor that never completes at all is invisible to the _failed bounds
    (gap_count stays 0) yet blocks the contiguous walk identically. Observed
    live on the derived pipeline: pending_completed grew to 11,858 while the
    watermark sat pinned, which kept requires_cursor_state() true for every new
    cursor and wrote ~10 rows/s into watermark_state forever."""

    async def run():
        sink = MemoryStore()
        manager = WatermarkManager(
            sink=sink,
            topic="checkpoint-topic",
            state_topic="watermark-state-topic",
            identity=_identity(),
            initial_cursor=99,
            flush_interval_ms=10000,
            commit_batch_size=100,
            max_gap_age_sec=0,
            max_gap_count=0,
            max_pending_completed=3,
        )

        await manager.mark_emitted(100)
        # 100 never completes (the hole); 101..103 complete out of order.
        for cursor in (101, 102, 103):
            await manager.mark_completed(cursor)

        assert manager.cursor == 99
        assert manager._failed == set(), "a hole is not a failure"
        assert len(manager._completed) == 3

        # The 4th pending cursor exceeds the bound -> the hole is skipped and
        # the watermark jumps to the true contiguous completion frontier.
        await manager.mark_completed(104)
        assert manager.cursor == 104
        assert manager._completed == set()

        await manager.stop(status="completed")

    asyncio.run(run())


def test_pending_hole_bound_can_be_disabled():
    async def run():
        sink = MemoryStore()
        manager = WatermarkManager(
            sink=sink,
            topic="checkpoint-topic",
            state_topic="watermark-state-topic",
            identity=_identity(),
            initial_cursor=99,
            flush_interval_ms=10000,
            commit_batch_size=100,
            max_gap_age_sec=0,
            max_gap_count=0,
            max_pending_completed=0,
        )

        await manager.mark_emitted(100)
        for cursor in (101, 102, 103, 104):
            await manager.mark_completed(cursor)

        # Bound disabled: the hole is never shed, so nothing above it commits.
        assert manager.cursor == 99
        assert len(manager._completed) == 4

        await manager.stop(status="completed")

    asyncio.run(run())
