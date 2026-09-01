from __future__ import annotations

import asyncio
import contextlib
import hashlib
import time
from dataclasses import dataclass
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
                message = consumer.poll(1.0)
                if message is None:
                    continue
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
                latest_record = CheckpointRecord(
                    cursor=int(value["cursor"]),
                    status=value.get("status", "running"),
                    updated_at_ms=int(value.get("updated_at_ms", 0)),
                    identity=self.identity,
                    error=value.get("error"),
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

    def load(self) -> dict[int, WatermarkStateRecord]:
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

        while len(seen_eof) < len(self._assigned_partitions):
            message = consumer.poll(1.0)
            if message is None:
                continue
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
                    return {
                        record.cursor: record
                        for record in self._records_by_key.values()
                    }
                raise
            record = WatermarkStateRecord(
                cursor=int(value["cursor"]),
                status=value.get("status", ""),
                updated_at_ms=int(value.get("updated_at_ms", 0)),
                identity=self.identity,
                error=value.get("error"),
            )
            self._records_by_key[key] = record

        records = {record.cursor: record for record in self._records_by_key.values()}
        if records and self.logger:
                self.logger.info(
                    "watermark.external_state_loaded",
                    topic=self.topic,
                    key=self.identity.key,
                    cursor_count=len(records),
                )
        return records

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

        started_at = time.perf_counter()
        delivery_future = await self.sink.send_checkpoint(
            self.topic,
            build_checkpoint_row(self.identity, cursor, status=status),
            wait_delivery=True,
        )
        if delivery_future is not None:
            await delivery_future
        self.last_delivery_wait_ms = round((time.perf_counter() - started_at) * 1000, 2)

    async def _flush_loop(self) -> None:
        while self._running:
            try:
                await asyncio.wait_for(self._flush_event.wait(), timeout=self.flush_interval)
            except asyncio.TimeoutError:
                pass
            self._flush_event.clear()
            await self.flush()

    async def _refresh_loop(self) -> None:
        while self._running:
            try:
                state_records = await asyncio.to_thread(self.state_reader.load)
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
