from __future__ import annotations

import json
import time
import warnings
from dataclasses import dataclass

from confluent_kafka import Consumer, KafkaError, Producer, TopicPartition, OFFSET_BEGINNING, OFFSET_END
from confluent_kafka.serialization import MessageField, SerializationContext

from rpcstream.sinks.kafka.protobuf import (
    DLQ_SCHEMA,
    ProtobufSerializerRegistry,
    build_message_class,
)


@dataclass
class DlqMessage:
    topic: str
    partition: int
    offset: int
    key: str | None
    value: dict
    raw_message: object


class UnifiedDlqKafkaClient:
    def __init__(
        self,
        *,
        topic: str,
        producer_config: dict,
        schema_registry_url: str,
        group_id: str,
        logger=None,
        auto_offset_reset: str = "earliest",
    ):
        self.topic = topic
        self.logger = logger
        self._consumer = Consumer(
            {
                **_kafka_client_config(producer_config),
                "group.id": group_id,
                "auto.offset.reset": auto_offset_reset,
                "enable.auto.commit": False,
                "isolation.level": "read_committed",
            }
        )
        self._producer = Producer(_kafka_client_config(producer_config))
        self._serializer_registry = ProtobufSerializerRegistry(
            schema_registry_url=schema_registry_url,
            producer_config=producer_config,
            topic_schemas={topic: DLQ_SCHEMA},
            auto_register_schemas=False,
            logger=logger,
        )
        self._serializer_registry.prepare()
        self._deserializer = _build_deserializer(schema_registry_url, producer_config)

    def subscribe(self) -> None:
        self._consumer.subscribe([self.topic])

    def wait_until_ready(self, timeout_sec: float = 10.0) -> None:
        deadline = time.monotonic() + timeout_sec
        while time.monotonic() < deadline:
            self._consumer.poll(0.2)
            if self._consumer.assignment():
                return
        raise RuntimeError(
            f"timed out after {timeout_sec}s waiting for assignment on topic={self.topic}"
        )

    def poll(self, timeout: float = 1.0) -> DlqMessage | None:
        message = self._consumer.poll(timeout)
        if message is None:
            return None
        if message.error():
            if message.error().code() == KafkaError._PARTITION_EOF:
                return None
            raise RuntimeError(str(message.error()))

        record = self._deserializer(
            message.value(),
            SerializationContext(message.topic(), MessageField.VALUE),
        )
        value = protobuf_message_to_dlq_record(record)
        key = message.key().decode("utf-8") if message.key() else None
        return DlqMessage(
            topic=message.topic(),
            partition=message.partition(),
            offset=message.offset(),
            key=key,
            value=value,
            raw_message=message,
        )

    def read_latest_records(
        self,
        *,
        offset_reset: str = "earliest",
        timeout_sec: float = 30.0,
    ) -> tuple[dict[str, dict], int]:
        partitions = self._topic_partitions()
        if not partitions:
            return {}, 0

        start_offset = OFFSET_BEGINNING if offset_reset == "earliest" else OFFSET_END
        assignments = [TopicPartition(self.topic, partition, start_offset) for partition in partitions]
        self._consumer.assign(assignments)

        high_watermarks = {
            partition: self._consumer.get_watermark_offsets(
                TopicPartition(self.topic, partition),
                timeout=10.0,
            )[1]
            for partition in partitions
        }

        latest_records: dict[str, dict] = {}
        scanned = 0
        deadline = time.monotonic() + timeout_sec
        done_partitions = {
            partition
            for partition, high in high_watermarks.items()
            if high == 0
        }

        while time.monotonic() < deadline and len(done_partitions) < len(partitions):
            message = self._consumer.poll(1.0)
            if message is not None:
                if message.error():
                    if message.error().code() == KafkaError._PARTITION_EOF:
                        done_partitions.add(message.partition())
                        continue
                    raise RuntimeError(str(message.error()))

                scanned += 1
                decoded = self._decode_message(message)
                latest_records[decoded.value["id"]] = decoded.value

            positions = self._consumer.position(
                [TopicPartition(self.topic, partition) for partition in partitions]
            )
            for position in positions:
                high = high_watermarks.get(position.partition)
                if high is None:
                    continue
                if position.offset >= high:
                    done_partitions.add(position.partition)

        return latest_records, scanned

    def commit(self, message: DlqMessage) -> None:
        self._consumer.commit(message.raw_message, asynchronous=False)

    def publish(self, record: dict) -> None:
        kafka_key = record.get("kafka_partition_key") or record.get("id")
        payload = self._serializer_registry.serialize(self.topic, record)
        self._producer.produce(
            topic=self.topic,
            key=kafka_key,
            value=payload,
        )
        self._producer.flush()

    def close(self) -> None:
        self._consumer.close()
        self._producer.flush()

    def _topic_partitions(self) -> list[int]:
        metadata = self._consumer.list_topics(self.topic, timeout=10.0)
        topic_metadata = metadata.topics.get(self.topic)
        if topic_metadata is None or topic_metadata.error is not None:
            return []
        return sorted(topic_metadata.partitions.keys())

    def _decode_message(self, message) -> DlqMessage:
        record = self._deserializer(
            message.value(),
            SerializationContext(message.topic(), MessageField.VALUE),
        )
        value = protobuf_message_to_dlq_record(record)
        key = message.key().decode("utf-8") if message.key() else None
        return DlqMessage(
            topic=message.topic(),
            partition=message.partition(),
            offset=message.offset(),
            key=key,
            value=value,
            raw_message=message,
        )


def protobuf_message_to_dlq_record(message) -> dict:
    record = {}
    for field in DLQ_SCHEMA.fields:
        value = getattr(message, field.name)
        if field.repeated:
            record[field.name] = list(value)
            continue

        if field.scalar_type == "string":
            record[field.name] = value or ""
        elif field.scalar_type == "int64":
            record[field.name] = int(value)
        elif field.scalar_type == "bool":
            record[field.name] = bool(value)
        else:
            record[field.name] = value

    for field_name in ("payload", "context"):
        raw = record.get(field_name)
        if raw:
            try:
                record[field_name] = json.loads(raw)
            except json.JSONDecodeError:
                record[field_name] = {"raw": raw}
        else:
            record[field_name] = {}

    if record.get("next_retry_at") == 0:
        record["next_retry_at"] = None
    return record


def _build_deserializer(schema_registry_url: str, producer_config: dict):
    with warnings.catch_warnings():
        try:
            from authlib.deprecate import AuthlibDeprecationWarning
        except Exception:
            AuthlibDeprecationWarning = DeprecationWarning

        warnings.filterwarnings(
            "ignore",
            category=AuthlibDeprecationWarning,
            module=r"authlib\._joserfc_helpers",
        )

        from confluent_kafka.schema_registry import SchemaRegistryClient
        from confluent_kafka.schema_registry.protobuf import ProtobufDeserializer

    client = SchemaRegistryClient(_schema_registry_conf(schema_registry_url, producer_config))
    return ProtobufDeserializer(
        build_message_class(DLQ_SCHEMA),
        schema_registry_client=client,
    )


def _schema_registry_conf(schema_registry_url: str, producer_config: dict) -> dict:
    username = producer_config.get("sasl.username")
    password = producer_config.get("sasl.password")
    conf = {"url": schema_registry_url}
    if username and password:
        conf["basic.auth.user.info"] = f"{username}:{password}"
    return conf


def _kafka_client_config(producer_config: dict) -> dict:
    allowed_prefixes = (
        "bootstrap.servers",
        "security.protocol",
        "sasl.",
        "ssl.",
    )
    return {
        key: value
        for key, value in producer_config.items()
        if any(key.startswith(prefix) for prefix in allowed_prefixes)
    }
