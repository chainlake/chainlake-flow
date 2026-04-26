from __future__ import annotations

import asyncio
import hashlib
import json
import time
from dataclasses import dataclass
from typing import Any


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
        return (
            f"pipeline={self.pipeline}|chain={self.chain_uid}|network={self.network}|"
            f"mode={self.mode}|unit={self.primary_unit}|entities={entity_key}"
        )


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


class KafkaCheckpointStore:
    def __init__(
        self,
        *,
        topic: str,
        producer_config: dict,
        identity: CheckpointIdentity,
        logger=None,
    ):
        self.topic = topic
        self.producer_config = producer_config
        self.identity = identity
        self.logger = logger
        self._producer = None

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

                value = json.loads(message.value().decode("utf-8"))
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
                component="checkpoint",
                topic=self.topic,
                key=self.identity.key,
                cursor=latest_record.cursor,
                status=latest_record.status,
            )
        return latest_record

    async def write(self, cursor: int, status: str = "running", error: str | None = None) -> None:
        await asyncio.to_thread(self._write_sync, cursor, status, error)

    def build_record(self, cursor: int, status: str = "running", error: str | None = None) -> tuple[str, bytes]:
        record = CheckpointRecord(
            cursor=cursor,
            status=status,
            updated_at_ms=int(time.time() * 1000),
            identity=self.identity,
            error=error,
        )
        return (
            self.identity.key,
            json.dumps(record.to_dict(), separators=(",", ":")).encode("utf-8"),
        )

    def _write_sync(self, cursor: int, status: str, error: str | None) -> None:
        from confluent_kafka import Producer

        if self._producer is None:
            self._producer = Producer(self.producer_config)
        key, value = self.build_record(cursor, status=status, error=error)
        self._producer.produce(
            topic=self.topic,
            key=key,
            value=value,
        )
        self._producer.flush()
        if self.logger:
            self.logger.info(
                "checkpoint.flushed",
                component="checkpoint",
                topic=self.topic,
                key=self.identity.key,
                cursor=cursor,
                status=status,
            )

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


class CheckpointManager:
    def __init__(
        self,
        *,
        store,
        initial_cursor: int | None = None,
        flush_interval_ms: int = 100,
        commit_batch_size: int = 100,
        logger=None,
    ):
        self.store = store
        self.cursor = initial_cursor
        self.flush_interval = flush_interval_ms / 1000
        self.commit_batch_size = commit_batch_size
        self.logger = logger
        self._completed = set()
        self._next_cursor = None if initial_cursor is None else initial_cursor + 1
        self._dirty = False
        self._running = False
        self._task = None
        self._lock = asyncio.Lock()
        self._flush_event = asyncio.Event()

    async def start(self) -> None:
        self._running = True
        self._task = asyncio.create_task(self._flush_loop())

    async def stop(self, status: str = "running") -> None:
        self._running = False
        self._flush_event.set()
        if self._task:
            await self._task
        await self.flush(status=status, force=True)

    async def mark_completed(self, cursor: int) -> None:
        async with self._lock:
            if self.cursor is not None and cursor <= self.cursor:
                return

            self._completed.add(cursor)
            self._advance_locked()

    async def mark_emitted(self, cursor: int) -> None:
        async with self._lock:
            if self._next_cursor is None:
                self._next_cursor = cursor
            self._advance_locked()

    def _advance_locked(self) -> None:
        if self._next_cursor is None:
            return

        advanced = 0
        while self._next_cursor in self._completed:
            self._completed.remove(self._next_cursor)
            self.cursor = self._next_cursor
            self._next_cursor += 1
            advanced += 1

        if advanced:
            self._dirty = True
            if advanced >= self.commit_batch_size:
                self._flush_event.set()

    async def mark_failed(self, cursor: int, error: str | None = None) -> None:
        if self.logger:
            self.logger.warn(
                "checkpoint.block_failed",
                component="checkpoint",
                cursor=cursor,
                error=error,
            )

    async def mark_eos(self) -> None:
        await self.flush(status="eos", force=True)

    async def flush(self, status: str = "running", force: bool = False) -> None:
        async with self._lock:
            if self.cursor is None:
                return
            if not self._dirty and not force:
                return
            cursor = self.cursor
            self._dirty = False

        await self.store.write(cursor, status=status)

    async def _flush_loop(self) -> None:
        while self._running:
            try:
                await asyncio.wait_for(self._flush_event.wait(), timeout=self.flush_interval)
            except asyncio.TimeoutError:
                pass
            self._flush_event.clear()
            await self.flush()
