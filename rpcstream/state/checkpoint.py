from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import time
from dataclasses import dataclass, field
from typing import Any

from confluent_kafka.serialization import MessageField, SerializationContext

from rpcstream.metrics.watermark import WatermarkMetrics
from rpcstream.sinks.kafka.protobuf import (
    CHECKPOINT_SCHEMA,
    WATERMARK_STATE_SCHEMA,
    SchemaRegistrySerializerRegistry,
    checkpoint_message_to_record,
    watermark_state_message_to_record,
)


@dataclass(frozen=True)
class CheckpointIdentity:
    pipeline: str
    chain_uid: str
    chain_type: str
    network: str
    mode: str
    primary_unit: str
    entities: tuple[str, ...]

    @property
    def key(self) -> str:
        entity_key = ",".join(sorted(self.entities))
        return f"pipeline={self.pipeline}|entities={entity_key}"

    @property
    def cursor_state_key_prefix(self) -> str:
        return self.key


@dataclass
class CheckpointRecord:
    cursor: int
    status: str
    updated_at_ms: int
    identity: CheckpointIdentity
    error: str | None = None
    # Non-empty when the checkpoint was written by a WatermarkManager that
    # supports inline inflight state. JSON list of {cursor,status,updated_at_ms}.
    cursor_state_snapshot: str | None = None

    def to_dict(self) -> dict[str, Any]:
        value = {
            "cursor": self.cursor,
            "status": self.status,
            "updated_at_ms": self.updated_at_ms,
            "pipeline": self.identity.pipeline,
            "chain_uid": self.identity.chain_uid,
            "chain_type": self.identity.chain_type,
            "network": self.identity.network,
            "mode": self.identity.mode,
            "primary_unit": self.identity.primary_unit,
            "entities": list(self.identity.entities),
        }
        if self.error:
            value["error"] = self.error
        return value


@dataclass
class WatermarkStateRecord:
    cursor: int
    status: str
    updated_at_ms: int
    identity: CheckpointIdentity
    error: str | None = None

    @property
    def key(self) -> str:
        return build_watermark_state_key(self.identity, self.cursor)

    def to_dict(self) -> dict[str, Any]:
        value = {
            "cursor": self.cursor,
            "status": self.status,
            "updated_at_ms": self.updated_at_ms,
            "pipeline": self.identity.pipeline,
            "chain_uid": self.identity.chain_uid,
            "chain_type": self.identity.chain_type,
            "network": self.identity.network,
            "mode": self.identity.mode,
            "primary_unit": self.identity.primary_unit,
            "entities": list(self.identity.entities),
        }
        if self.error:
            value["error"] = self.error
        return value


def build_checkpoint_row(
    identity: "CheckpointIdentity",
    cursor: int,
    status: str = "running",
    error: str | None = None,
    updated_at_ms: int | None = None,
) -> dict[str, Any]:
    record = CheckpointRecord(
        cursor=cursor,
        status=status,
        updated_at_ms=updated_at_ms or int(time.time() * 1000),
        identity=identity,
        error=error,
    )
    payload = record.to_dict()
    payload["id"] = identity.key
    payload["kafka_partition_key"] = identity.key
    return payload


def build_watermark_state_key(identity: "CheckpointIdentity", cursor: int) -> str:
    return f"{identity.cursor_state_key_prefix}|cursor={cursor}"


def build_watermark_state_row(
    identity: "CheckpointIdentity",
    cursor: int,
    status: str,
    error: str | None = None,
    updated_at_ms: int | None = None,
) -> dict[str, Any]:
    record = WatermarkStateRecord(
        cursor=cursor,
        status=status,
        updated_at_ms=updated_at_ms or int(time.time() * 1000),
        identity=identity,
        error=error,
    )
    payload = record.to_dict()
    payload["id"] = record.key
    payload["kafka_partition_key"] = record.key
    return payload


def build_checkpoint_identity(runtime) -> CheckpointIdentity:
    primary_unit = "block"
    if runtime.chain.type == "sui":
        primary_unit = "checkpoint"

    return CheckpointIdentity(
        pipeline=runtime.pipeline.name,
        chain_uid=runtime.chain.uid,
        chain_type=runtime.chain.type,
        network=runtime.chain.network,
        mode=runtime.pipeline.mode,
        primary_unit=primary_unit,
        entities=tuple(runtime.entities),
    )


def _is_missing_schema_error(exc: Exception) -> bool:
    message = str(exc).lower()
    return "schema" in message and ("not found" in message or "40403" in message)


class KafkaCheckpointReader:
    def __init__(
        self,
        *,
        topic: str,
        producer_config: dict,
        identity: CheckpointIdentity,
        schema_registry_url: str | None = None,
        schema_registry_type: str = "protobuf",
        logger=None,
    ):
        self.topic = topic
        self.producer_config = producer_config
        self.identity = identity
        self.schema_registry_url = schema_registry_url
        self.schema_registry_type = schema_registry_type
        self.logger = logger
        self._producer = None
        self._serializer_registry = None
        self._deserializer = None
        self.schema_missing = False

        if self.schema_registry_url:
            self._serializer_registry = SchemaRegistrySerializerRegistry(
                schema_registry_url=self.schema_registry_url,
                producer_config=self.producer_config,
                topic_schemas={self.topic: CHECKPOINT_SCHEMA},
                auto_register_schemas=False,
                logger=logger,
                schema_format=self.schema_registry_type,
            )
            self._serializer_registry.prepare()
            self._deserializer = self._serializer_registry.build_deserializer(self.topic)

    def load(self) -> CheckpointRecord | None:
        from confluent_kafka import Consumer, KafkaError, TopicPartition

        consumer = Consumer(self._consumer_config())
        latest_record = None
        try:
            metadata = consumer.list_topics(self.topic, timeout=10)
            topic_meta = metadata.topics.get(self.topic)
            if topic_meta is None or topic_meta.error is not None:
                return None

            partitions = [
                TopicPartition(self.topic, partition)
                for partition in topic_meta.partitions
            ]
            if not partitions:
                return None

            low_high = {}
            seen_eof = set()
            for tp in partitions:
                low, high = consumer.get_watermark_offsets(tp, timeout=10)
                low_high[tp.partition] = (low, high)
                if high <= low:
                    seen_eof.add(tp.partition)

            if len(seen_eof) == len(partitions):
                return None

            consumer.assign(partitions)

            while len(seen_eof) < len(partitions):
                # consume() returns up to 500 messages per call instead of
                # one. This turns ~1.2M poll() round-trips (each ~35 µs of
                # Python overhead) into ~2400 consume() calls -- cutting the
                # cold-start checkpoint scan from ~26s down to under 1s.
                messages = consumer.consume(num_messages=500, timeout=1.0)
                for message in messages:
                    if message.error():
                        if message.error().code() == KafkaError._PARTITION_EOF:
                            seen_eof.add(message.partition())
                            continue
                        raise RuntimeError(message.error())

                    high = low_high.get(message.partition(), (0, 0))[1]
                    if message.offset() >= high - 1:
                        seen_eof.add(message.partition())

                    if message.key() is None or message.value() is None:
                        continue
                    if message.key().decode("utf-8") != self.identity.key:
                        continue

                    try:
                        value = self._decode_record(message.value())
                    except Exception as exc:
                        if _is_missing_schema_error(exc):
                            self.schema_missing = True
                            if self.logger:
                                self.logger.warn(
                                    "checkpoint.schema_missing",
                                    topic=self.topic,
                                    key=self.identity.key,
                                    error=str(exc),
                                )
                            return None
                        raise
                    snapshot = value.get("cursor_state_snapshot") or None
                    latest_record = CheckpointRecord(
                        cursor=int(value["cursor"]),
                        status=value.get("status", "running"),
                        updated_at_ms=int(value.get("updated_at_ms", 0)),
                        identity=self.identity,
                        error=value.get("error"),
                        cursor_state_snapshot=snapshot,
                    )
        finally:
            consumer.close()

        if latest_record and self.logger:
            self.logger.info(
                "checkpoint.loaded",
                topic=self.topic,
                key=self.identity.key,
                cursor=latest_record.cursor,
                status=latest_record.status,
            )
        return latest_record

    def _decode_record(self, payload: bytes) -> dict[str, Any]:
        if self._deserializer is None:
            import json
            return json.loads(payload.decode("utf-8"))

        message = self._deserializer(
            payload,
            SerializationContext(self.topic, MessageField.VALUE),
        )
        if self.schema_registry_type == "avro":
            return message
        return checkpoint_message_to_record(message)

    def _schema_registry_conf(self) -> dict:
        username = self.producer_config.get("sasl.username")
        password = self.producer_config.get("sasl.password")
        conf = {"url": self.schema_registry_url}
        if username and password:
            conf["basic.auth.user.info"] = f"{username}:{password}"
        return conf

    def _consumer_config(self) -> dict:
        allowed_prefixes = (
            "bootstrap.servers",
            "security.protocol",
            "sasl.",
            "ssl.",
        )
        config = {
            key: value
            for key, value in self.producer_config.items()
            if any(key.startswith(prefix) for prefix in allowed_prefixes)
        }
        config.update(
            {
                "group.id": f"checkpoint-loader-{hashlib.sha256(self.identity.key.encode()).hexdigest()}",
                "enable.auto.commit": False,
                "enable.partition.eof": True,
                "isolation.level": "read_committed",
                "auto.offset.reset": "earliest",
            }
        )
        return config


class KafkaWatermarkStateReader:
    def __init__(
        self,
        *,
        topic: str,
        producer_config: dict,
        identity: CheckpointIdentity,
        schema_registry_url: str | None = None,
        schema_registry_type: str = "protobuf",
        logger=None,
    ):
        self.topic = topic
        self.producer_config = producer_config
        self.identity = identity
        self.schema_registry_url = schema_registry_url
        self.schema_registry_type = schema_registry_type
        self.logger = logger
        self._serializer_registry = None
        self._deserializer = None
        self.schema_missing = False

        # `load()` is called repeatedly for the life of the process (the
        # WatermarkManager refresh loop polls it roughly every second). A
        # brand-new Consumer + a from-`auto.offset.reset=earliest` scan on
        # every call re-reads and re-decodes the ENTIRE topic from scratch
        # each time -- with `enable.auto.commit: False` and no seek, there's
        # no persisted position to resume from. This topic is compacted but
        # compaction lags real time (only runs over closed segments), so the
        # raw record count grows far faster than the deduplicated state
        # actually needed: live, this topic held ~207k raw messages for
        # ~13k distinct cursor keys, so every refresh was re-decoding ~15x
        # more Avro records than useful, taking 30+ seconds and getting
        # slower as the topic grew -- the real driver behind rpcstream
        # falling behind, not Kafka message serialization.
        #
        # Keeping the Consumer object alive across calls (instead of a new
        # one each time) makes subsequent polls resume from wherever the
        # previous call's polling left off for free -- confluent_kafka
        # tracks the next-fetch position per assigned partition internally
        # regardless of whether offsets are committed. Committing only
        # affects externally-visible position for group rebalancing, not
        # this in-process fetch cursor, so this needs no offset-commit
        # changes at all.
        self._consumer = None
        self._assigned_partitions = None
        self._records_by_key: dict[str, WatermarkStateRecord] = {}
        # Written by load() (inside the worker thread) after each scan.
        # Read by WatermarkManager._refresh_loop() (event loop) after to_thread()
        # returns — the happens-before guarantee of to_thread() makes this safe.
        self._last_consumer_positions: dict[int, int] = {}

        if self.schema_registry_url:
            self._serializer_registry = SchemaRegistrySerializerRegistry(
                schema_registry_url=self.schema_registry_url,
                producer_config=self.producer_config,
                topic_schemas={self.topic: WATERMARK_STATE_SCHEMA},
                auto_register_schemas=False,
                logger=logger,
                schema_format=self.schema_registry_type,
            )
            self._serializer_registry.prepare()
            self._deserializer = self._serializer_registry.build_deserializer(self.topic)

    def load(self, committed_cursor: int | None = None) -> dict[int, WatermarkStateRecord]:
        from confluent_kafka import KafkaError, TopicPartition

        if self._consumer is None:
            from confluent_kafka import Consumer

            consumer = Consumer(self._consumer_config())
            metadata = consumer.list_topics(self.topic, timeout=10)
            topic_meta = metadata.topics.get(self.topic)
            if topic_meta is None or topic_meta.error is not None:
                consumer.close()
                return {}

            partitions = [
                TopicPartition(self.topic, partition)
                for partition in topic_meta.partitions
            ]
            if not partitions:
                consumer.close()
                return {}

            consumer.assign(partitions)
            self._consumer = consumer
            self._assigned_partitions = partitions

        consumer = self._consumer
        prefix = f"{self.identity.cursor_state_key_prefix}|cursor="

        low_high = {}
        seen_eof = set()
        for tp in self._assigned_partitions:
            low, high = consumer.get_watermark_offsets(tp, timeout=10, cached=False)
            low_high[tp.partition] = (low, high)
            position = consumer.position([tp])[0].offset
            # No fetch has happened yet on this partition (position unset,
            # a negative sentinel) -- there's nothing to compare against
            # `high` yet, so just check whether the topic is empty outright.
            if position < 0:
                if high <= low:
                    seen_eof.add(tp.partition)
            elif position >= high:
                seen_eof.add(tp.partition)

        # Only the records actually read in this call — the delta.
        # On the first call _records_by_key is empty so every record is new
        # (old is None), and newly_read == the full bootstrap snapshot.
        # On subsequent calls only truly new/changed records end up here, so
        # merge_external_state_records processes a tiny delta instead of the
        # ever-growing full set, eliminating the O(n) sort + dict-build per tick.
        newly_read: dict[int, WatermarkStateRecord] = {}

        while len(seen_eof) < len(self._assigned_partitions):
            # Same batch-consume optimisation as KafkaCheckpointReader: cuts
            # ~1.6M one-at-a-time poll() calls (~42s) down to ~3200 consume()
            # batch calls (under 1s) for the cold-start full topic scan.
            messages = consumer.consume(num_messages=500, timeout=1.0)
            for message in messages:
                if message.error():
                    if message.error().code() == KafkaError._PARTITION_EOF:
                        seen_eof.add(message.partition())
                        continue
                    raise RuntimeError(message.error())

                high = low_high.get(message.partition(), (0, 0))[1]
                if message.offset() >= high - 1:
                    seen_eof.add(message.partition())

                if message.key() is None or message.value() is None:
                    continue

                key = message.key().decode("utf-8")
                if not key.startswith(prefix):
                    continue

                try:
                    value = self._decode_record(message.value())
                except Exception as exc:
                    if _is_missing_schema_error(exc):
                        self.schema_missing = True
                        if self.logger:
                            self.logger.warn(
                                "watermark.schema_missing",
                                topic=self.topic,
                                key=self.identity.key,
                                error=str(exc),
                            )
                        return newly_read
                    raise
                record = WatermarkStateRecord(
                    cursor=int(value["cursor"]),
                    status=value.get("status", ""),
                    updated_at_ms=int(value.get("updated_at_ms", 0)),
                    identity=self.identity,
                    error=value.get("error"),
                )
                old = self._records_by_key.get(key)
                self._records_by_key[key] = record
                if old is None or old.updated_at_ms != record.updated_at_ms:
                    newly_read[record.cursor] = record

        # Capture the consumer's read position while still on the worker thread.
        # Stored so WatermarkManager can embed it in the next checkpoint, letting
        # cold start seek directly here instead of re-scanning from offset 0.
        new_positions: dict[int, int] = {}
        for tp in self._assigned_partitions:
            try:
                pos_list = consumer.position([tp])
                if pos_list and pos_list[0].offset >= 0:
                    new_positions[tp.partition] = pos_list[0].offset
            except Exception:
                pass
        if new_positions:
            self._last_consumer_positions = new_positions

        # Prune _records_by_key for committed cursors — those entries are
        # unreachable by future merge_external_state_records calls (the manager
        # skips cursor <= self.cursor), so keeping them wastes memory. This
        # keeps the dict bounded to the inflight window rather than all-time.
        if committed_cursor is not None:
            keys_to_delete = [
                k for k, v in self._records_by_key.items()
                if v.cursor <= committed_cursor
            ]
            for k in keys_to_delete:
                del self._records_by_key[k]

        if newly_read and self.logger:
            self.logger.debug(
                "watermark.external_state_loaded",
                topic=self.topic,
                key=self.identity.key,
                cursor_count=len(self._records_by_key),
                new_records=len(newly_read),
            )
        return newly_read

    def fast_init(self, positions: dict[int, int] | None = None) -> None:
        """Seek consumer to saved positions (or topic head) without scanning.

        Called on cold start when cursor_state_snapshot in the checkpoint
        carries the consumer's last read positions. Seeking there means the
        first _refresh_loop call reads only records written after the
        checkpoint, skipping the full O(N) bootstrap scan.

        `positions` maps partition → next-fetch offset, as captured by load()
        after the previous pod's last scan. If None (legacy fallback), seeks
        each partition to its current high watermark (topic head).
        """
        from confluent_kafka import Consumer, TopicPartition

        if self._consumer is not None:
            return

        consumer = Consumer(self._consumer_config())
        metadata = consumer.list_topics(self.topic, timeout=10)
        topic_meta = metadata.topics.get(self.topic)
        if topic_meta is None or topic_meta.error is not None:
            consumer.close()
            return

        partitions = [
            TopicPartition(self.topic, partition)
            for partition in topic_meta.partitions
        ]
        if not partitions:
            consumer.close()
            return

        consumer.assign(partitions)
        for tp in partitions:
            target: int | None = positions.get(tp.partition) if positions is not None else None
            if target is None:
                _, high = consumer.get_watermark_offsets(tp, timeout=10)
                target = high if high > 0 else 0
            consumer.seek(TopicPartition(self.topic, tp.partition, target))

        self._consumer = consumer
        self._assigned_partitions = partitions

    def close(self) -> None:
        if self._consumer is not None:
            self._consumer.close()
            self._consumer = None

    def _decode_record(self, payload: bytes) -> dict[str, Any]:
        if self._deserializer is None:
            import json
            return json.loads(payload.decode("utf-8"))

        message = self._deserializer(
            payload,
            SerializationContext(self.topic, MessageField.VALUE),
        )
        if self.schema_registry_type == "avro":
            return message
        return watermark_state_message_to_record(message)

    def _schema_registry_conf(self) -> dict:
        username = self.producer_config.get("sasl.username")
        password = self.producer_config.get("sasl.password")
        conf = {"url": self.schema_registry_url}
        if username and password:
            conf["basic.auth.user.info"] = f"{username}:{password}"
        return conf

    def _consumer_config(self) -> dict:
        allowed_prefixes = (
            "bootstrap.servers",
            "security.protocol",
            "sasl.",
            "ssl.",
        )
        config = {
            key: value
            for key, value in self.producer_config.items()
            if any(key.startswith(prefix) for prefix in allowed_prefixes)
        }
        config.update(
            {
                "group.id": f"watermark-loader-{hashlib.sha256(self.identity.key.encode()).hexdigest()}",
                "enable.auto.commit": False,
                "enable.partition.eof": True,
                "isolation.level": "read_committed",
                "auto.offset.reset": "earliest",
            }
        )
        return config


class WatermarkManager:
    def __init__(
        self,
        *,
        sink,
        topic: str,
        state_topic: str,
        identity: CheckpointIdentity,
        initial_cursor: int | None = None,
        state_records: dict[int, WatermarkStateRecord] | None = None,
        state_reader: KafkaWatermarkStateReader | None = None,
        flush_interval_ms: int = 100,
        commit_batch_size: int = 100,
        flush_on_advance: bool = True,
        state_refresh_interval_ms: int = 1000,
        logger=None,
        meter=None,
    ):
        self.sink = sink
        self.topic = topic
        self.state_topic = state_topic
        self.identity = identity
        self.cursor = initial_cursor
        self.state_reader = state_reader
        self.flush_interval = flush_interval_ms / 1000
        self.state_refresh_interval = state_refresh_interval_ms / 1000
        self.commit_batch_size = commit_batch_size
        self.logger = logger
        self.flush_on_advance = flush_on_advance
        self._completed = set()
        self._failed = set()
        self._next_cursor = None if initial_cursor is None else initial_cursor + 1
        self._dirty = False
        self._running = False
        self._flush_task = None
        self._refresh_task = None
        self._lock = asyncio.Lock()
        self._flush_event = asyncio.Event()
        self._state_versions: dict[int, tuple[int, str]] = {}
        # Cursors pruned from _state_versions on advance — need tombstones
        # written to state_topic so compaction can remove their keys.
        self._pending_tombstones: list[int] = []
        # Consumer read positions from the last state_reader.load() call.
        # Embedded in each checkpoint so cold start can seek past old records.
        self._last_state_consumer_positions: dict[int, int] = {}
        self.last_delivery_wait_ms: float | None = None
        self.metrics = WatermarkMetrics(
            meter,
            attributes={
                "pipeline": identity.pipeline,
                "chain_uid": identity.chain_uid,
                "chain_type": identity.chain_type,
                "network": identity.network,
                "mode": identity.mode,
                "primary_unit": identity.primary_unit,
            },
        )
        self._hydrate_state_records(state_records or {})
        self._refresh_metrics()

    async def start(self) -> None:
        self._running = True
        if self.flush_on_advance:
            self._flush_task = asyncio.create_task(self._flush_loop())
        if self.state_reader is not None:
            self._refresh_task = asyncio.create_task(self._refresh_loop())

    async def stop(self, status: str = "running") -> None:
        self._running = False
        self._flush_event.set()
        if self._flush_task:
            await self._flush_task
        if self._refresh_task:
            self._refresh_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, asyncio.TimeoutError):
                await asyncio.wait_for(self._refresh_task, timeout=0.1)
        if self.flush_on_advance:
            await self.flush(status=status, force=True)
        if self.state_reader is not None and hasattr(self.state_reader, "close"):
            await asyncio.to_thread(self.state_reader.close)

    async def mark_completed(self, cursor: int) -> int | None:
        async with self._lock:
            if self.cursor is not None and cursor <= self.cursor:
                return None

            self._completed.add(cursor)
            self._failed.discard(cursor)
            advanced = self._advance_locked()
            self._refresh_metrics()
            return advanced

    async def preview_completed(self, cursor: int) -> int | None:
        async with self._lock:
            if self.cursor is not None and cursor <= self.cursor:
                return None
            completed = set(self._completed)
            completed.add(cursor)
            return self._preview_advance_locked(completed)

    async def mark_emitted(self, cursor: int) -> int | None:
        async with self._lock:
            if self._next_cursor is None:
                self._next_cursor = cursor
            advanced = self._advance_locked()
            self._refresh_metrics()
            return advanced

    async def requires_cursor_state(self, cursor: int) -> bool:
        async with self._lock:
            if cursor in self._failed:
                return True
            if self._next_cursor is None:
                return False
            return cursor > self._next_cursor

    def _preview_advance_locked(self, completed: set[int]) -> int | None:
        next_cursor = self._next_cursor
        if next_cursor is None:
            if not completed:
                return None
            next_cursor = min(completed)

        advanced_cursor = None
        while next_cursor in completed:
            completed.remove(next_cursor)
            advanced_cursor = next_cursor
            next_cursor += 1
        return advanced_cursor

    def _hydrate_state_records(self, state_records: dict[int, WatermarkStateRecord]) -> None:
        if not state_records:
            return

        for cursor, record in sorted(state_records.items()):
            self._state_versions[cursor] = (record.updated_at_ms, record.status)
            if self.cursor is not None and cursor <= self.cursor:
                continue
            if record.status == "completed":
                self._completed.add(cursor)
                self._failed.discard(cursor)
            elif record.status == "failed":
                self._failed.add(cursor)

        self._advance_locked()
        self._refresh_metrics()

    def _advance_locked(self) -> int | None:
        if self._next_cursor is None:
            return None

        advanced = 0
        advanced_cursor = None
        while self._next_cursor in self._completed:
            self._completed.remove(self._next_cursor)
            self.cursor = self._next_cursor
            self._next_cursor += 1
            advanced += 1
            advanced_cursor = self.cursor

        if advanced:
            self._dirty = True
            if self.flush_on_advance and advanced >= self.commit_batch_size:
                self._flush_event.set()
            # Prune _state_versions for committed cursors — the version
            # deduplication check in merge_external_state_records already
            # skips cursor <= self.cursor before it reaches _state_versions,
            # so these entries are dead weight. Without pruning this dict
            # grows at block-rate indefinitely, causing OOMKill over hours.
            if self.cursor is not None and self._state_versions:
                committed = self.cursor
                to_tombstone = [c for c in self._state_versions if c <= committed]
                if to_tombstone:
                    self._pending_tombstones.extend(to_tombstone)
                self._state_versions = {
                    c: v for c, v in self._state_versions.items()
                    if c > committed
                }
        return advanced_cursor

    async def mark_failed(self, cursor: int, error: str | None = None) -> None:
        async with self._lock:
            if self.cursor is not None and cursor <= self.cursor:
                return
            self._failed.add(cursor)
            self._refresh_metrics()
        self.metrics.record_cursor_failed()
        if self.logger:
            self.logger.warn(
                "watermark.cursor_failed",
                cursor=cursor,
                error=error,
            )

    async def merge_external_state_records(
        self,
        state_records: dict[int, WatermarkStateRecord],
    ) -> int | None:
        async with self._lock:
            advanced_watermark = None
            for cursor, record in sorted(state_records.items()):
                previous = self._state_versions.get(cursor)
                current_version = (record.updated_at_ms, record.status)
                if previous is not None and current_version <= previous:
                    continue
                self._state_versions[cursor] = current_version

                if self.cursor is not None and cursor <= self.cursor:
                    continue
                if record.status == "completed":
                    self._completed.add(cursor)
                    self._failed.discard(cursor)
                elif record.status == "failed":
                    self._failed.add(cursor)

            advanced_watermark = self._advance_locked()
            self._refresh_metrics()
            return advanced_watermark

    def update_commit_delay(self, delay: int | None) -> None:
        self.metrics.update(commit_delay=delay)

    def set_backfill_range(self, start: int | None, target: int | None) -> None:
        """Publish this bounded backfill's segment bounds (pipeline.from/to)
        as per-instance gauges. Called by the engine once it knows it is in
        backfill mode; realtime processes never call this, so no backfill
        gauge series is exported for them. Values are static for the run --
        the segment's configured range does not change under checkpoint
        resume (resuming still belongs to the same segment)."""
        self.metrics.update(start_cursor=start, target_cursor=target)

    def get_metrics_snapshot(self) -> dict[str, int | None]:
        return self.metrics.snapshot()

    def _refresh_metrics(self) -> None:
        oldest_gap = min(self._failed) if self._failed else None
        self.metrics.update(
            commit_cursor=self.cursor,
            gap_count=len(self._failed),
            oldest_gap=oldest_gap,
        )

    async def mark_completed_run(self) -> None:
        await self.flush(status="completed", force=True)

    async def mark_eos(self) -> None:
        await self.mark_completed_run()

    async def flush(self, status: str = "running", force: bool = False) -> None:
        async with self._lock:
            if self.cursor is None:
                return
            if not self._dirty and not force:
                return
            cursor = self.cursor
            self._dirty = False
            # Embed the state_reader consumer's last read position so cold start
            # can seek directly there instead of re-scanning from offset 0.
            # For a realtime pipeline the inflight window (_state_versions) is
            # typically empty (in-order completions leave nothing to snapshot),
            # but the consumer position is always valid after the first scan.
            positions = self._last_state_consumer_positions
            _snapshot = (
                json.dumps({str(p): o for p, o in positions.items()})
                if positions else ""
            )

        started_at = time.perf_counter()
        row = build_checkpoint_row(self.identity, cursor, status=status)
        row["cursor_state_snapshot"] = _snapshot
        delivery_future = await self.sink.send_checkpoint(
            self.topic,
            row,
            wait_delivery=True,
        )
        if delivery_future is not None:
            # Bound the wait: a permanently unresolvable future (sink worker
            # crashed between enqueue and produce, no callback registered)
            # would otherwise block stop() → run_stream's finally block
            # indefinitely. 30s matches sink_failure_timeout_sec on the engine
            # side; the checkpoint write is low-priority compared to not hanging.
            try:
                await asyncio.wait_for(delivery_future, timeout=30.0)
            except (asyncio.TimeoutError, Exception):
                if self.logger:
                    self.logger.warn("watermark.checkpoint_flush_timeout", cursor=cursor)
        self.last_delivery_wait_ms = round((time.perf_counter() - started_at) * 1000, 2)

    async def _flush_loop(self) -> None:
        while self._running:
            try:
                await asyncio.wait_for(self._flush_event.wait(), timeout=self.flush_interval)
            except asyncio.TimeoutError:
                pass
            self._flush_event.clear()
            await self.flush()
            await self._write_tombstones()

    async def _write_tombstones(self) -> None:
        """Write null-value tombstones for recently-committed cursor state keys.

        Tombstones tell Kafka/Redpanda log compaction to delete the key from
        the cursor_state topic. Without them, committed cursor keys accumulate
        forever — a 120M-block backfill would leave 120M live keys, making
        every cold-start scan O(N) even after compaction.

        Fire-and-forget: a lost tombstone means the key lingers until the next
        checkpoint cycle writes it again; correctness is not affected.
        """
        if not self._pending_tombstones:
            return
        tombstones, self._pending_tombstones = self._pending_tombstones, []
        sink = self.sink
        if not hasattr(sink, "send_tombstone"):
            return
        for cursor in tombstones:
            key = build_watermark_state_key(self.identity, cursor)
            sink.send_tombstone(self.state_topic, key)

    async def _refresh_loop(self) -> None:
        while self._running:
            try:
                committed = self.cursor
                state_records = await asyncio.to_thread(
                    self.state_reader.load, committed
                )
                # to_thread() provides happens-before for all writes inside
                # load(), so reading _last_consumer_positions here is safe.
                positions = getattr(self.state_reader, "_last_consumer_positions", {})
                if positions:
                    self._last_state_consumer_positions = positions
                if state_records:
                    advanced_watermark = await self.merge_external_state_records(state_records)
                    if advanced_watermark is not None and self.logger is not None:
                        self.logger.info(
                            "watermark.external_state_merged",
                            cursor=advanced_watermark,
                            topic=self.state_reader.topic,
                        )
            except Exception as exc:
                if self.logger is not None:
                    self.logger.warn(
                        "watermark.external_state_refresh_failed",
                        topic=getattr(self.state_reader, "topic", None),
                        error=str(exc),
                    )
            await asyncio.sleep(self.state_refresh_interval)


CheckpointManager = WatermarkManager


def checkpoint_message_to_record(message) -> dict[str, Any]:
    record = {}
    for field in CHECKPOINT_SCHEMA.fields:
        value = getattr(message, field.name)
        if field.repeated:
            record[field.name] = list(value)
            continue
        if field.scalar_type == "string":
            record[field.name] = value or ""
        elif field.scalar_type == "int64":
            record[field.name] = int(value)
        else:
            record[field.name] = value

    if record.get("error") == "":
        record["error"] = None
    return record
