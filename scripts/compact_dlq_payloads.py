#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import json
import sys
import time
from dataclasses import dataclass
from pathlib import Path

from confluent_kafka import Consumer, KafkaError, Producer, TopicPartition
from confluent_kafka.admin import AdminClient
from confluent_kafka.serialization import MessageField, SerializationContext


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from rpcstream.config.loader import load_pipeline_config  # noqa: E402
from rpcstream.config.resolver import resolve  # noqa: E402
from rpcstream.ingestion.dlq import summarize_payload  # noqa: E402
from rpcstream.sinks.kafka.dlq import (  # noqa: E402
    _build_deserializer,
    _kafka_client_config,
    protobuf_message_to_dlq_record,
)
from rpcstream.sinks.kafka.protobuf import DLQ_SCHEMA, ProtobufSerializerRegistry  # noqa: E402


@dataclass
class SnapshotRecord:
    partition: int
    offset: int
    key: bytes | None
    value: dict


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Rewrite unified DLQ records so payload contains only a compact summary."
    )
    parser.add_argument(
        "--config",
        default=str(REPO_ROOT / "rpcstream" / "pipeline.yaml"),
        help="Path to rpcstream pipeline.yaml.",
    )
    parser.add_argument("--apply", action="store_true")
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Required with --apply because this deletes old DLQ records before republishing.",
    )
    parser.add_argument("--poll-timeout-sec", type=float, default=1.0)
    parser.add_argument("--idle-polls", type=int, default=3)
    parser.add_argument(
        "--snapshot-file",
        default=None,
        help="Where to write the transformed JSONL snapshot before deleting old records.",
    )
    parser.add_argument(
        "--restore-from",
        default=None,
        help="Publish records from a previously written JSONL snapshot without deleting records.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    config_path = Path(args.config).resolve()
    runtime = resolve(load_pipeline_config(str(config_path)))
    topic = runtime.topic_map.dlq
    if not runtime.kafka.schema_registry_url:
        raise SystemExit("protobuf DLQ decoding requires KAFKA_SCHEMA_REGISTRY or KAFAK_SCHEMA_REGISTRY")

    kafka_config = _kafka_client_config(runtime.kafka.config)
    admin = AdminClient(kafka_config)

    if args.restore_from:
        records = read_snapshot_file(Path(args.restore_from))
        if not args.apply:
            print(
                json.dumps(
                    {
                        "topic": topic,
                        "records": len(records),
                        "restore_from": args.restore_from,
                        "dry_run": True,
                    },
                    sort_keys=True,
                )
            )
            return 0
        publish_records(runtime, topic, records)
        print(json.dumps({"topic": topic, "republished": len(records)}, sort_keys=True))
        return 0

    partitions = list_topic_partitions(admin, topic)
    consumer = Consumer(
        {
            **kafka_config,
            "group.id": f"rpcstream-dlq-payload-migration-{int(time.time())}",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    deserializer = _build_deserializer(runtime.kafka.schema_registry_url, runtime.kafka.config)

    try:
        watermarks = snapshot_watermarks(consumer, topic, partitions)
        records = read_snapshot(
            consumer,
            deserializer,
            topic,
            watermarks,
            poll_timeout_sec=args.poll_timeout_sec,
            idle_polls=args.idle_polls,
        )
    finally:
        consumer.close()

    transformed = [transform_record(record) for record in records]
    changed = sum(1 for before, after in zip(records, transformed) if before.value != after.value)
    snapshot_file = Path(args.snapshot_file) if args.snapshot_file else None
    print(
        json.dumps(
            {
                "topic": topic,
                "partitions": partitions,
                "watermarks": watermarks,
                "records": len(records),
                "changed": changed,
                "dry_run": not args.apply,
                "snapshot_file": str(snapshot_file) if snapshot_file else None,
            },
            sort_keys=True,
        )
    )

    if not args.apply:
        print("dry run only; rerun with --apply --yes to delete and republish")
        return 0
    if not args.yes:
        raise SystemExit("--apply requires --yes")

    if snapshot_file is None:
        snapshot_file = REPO_ROOT / "scripts" / f"dlq_payload_snapshot_{int(time.time())}.jsonl"
    write_snapshot_file(snapshot_file, transformed)
    print(f"snapshot_written {snapshot_file}")

    delete_snapshot_records(admin, topic, watermarks)
    publish_records(runtime, topic, transformed)
    print(
        json.dumps(
            {
                "topic": topic,
                "deleted_before_offsets": {str(partition): high for partition, (_low, high) in watermarks.items()},
                "republished": len(transformed),
            },
            sort_keys=True,
        )
    )
    return 0


def list_topic_partitions(admin: AdminClient, topic: str) -> list[int]:
    metadata = admin.list_topics(topic=topic, timeout=30)
    topic_meta = metadata.topics.get(topic)
    if topic_meta is None:
        raise SystemExit(f"topic not found: {topic}")
    if topic_meta.error is not None:
        raise SystemExit(f"topic metadata error for {topic}: {topic_meta.error}")
    return sorted(topic_meta.partitions)


def snapshot_watermarks(
    consumer: Consumer,
    topic: str,
    partitions: list[int],
) -> dict[int, tuple[int, int]]:
    result = {}
    for partition in partitions:
        low, high = consumer.get_watermark_offsets(
            TopicPartition(topic, partition),
            timeout=30,
        )
        result[partition] = (low, high)
    return result


def read_snapshot(
    consumer: Consumer,
    deserializer,
    topic: str,
    watermarks: dict[int, tuple[int, int]],
    *,
    poll_timeout_sec: float,
    idle_polls: int,
) -> list[SnapshotRecord]:
    assignments = [
        TopicPartition(topic, partition, low)
        for partition, (low, high) in watermarks.items()
        if high > low
    ]
    if not assignments:
        return []

    consumer.assign(assignments)
    done = {partition for partition, (low, high) in watermarks.items() if high <= low}
    records: list[SnapshotRecord] = []
    idle_count = 0

    while len(done) < len(watermarks):
        message = consumer.poll(poll_timeout_sec)
        if message is None:
            idle_count += 1
            if idle_count >= idle_polls:
                break
            continue
        idle_count = 0

        if message.error():
            if message.error().code() == KafkaError._PARTITION_EOF:
                done.add(message.partition())
                continue
            raise RuntimeError(str(message.error()))

        partition = message.partition()
        high = watermarks[partition][1]
        if message.offset() >= high:
            done.add(partition)
            continue
        if message.value() is None:
            continue

        decoded = deserializer(
            message.value(),
            SerializationContext(message.topic(), MessageField.VALUE),
        )
        records.append(
            SnapshotRecord(
                partition=partition,
                offset=message.offset(),
                key=message.key(),
                value=protobuf_message_to_dlq_record(decoded),
            )
        )
        if message.offset() >= high - 1:
            done.add(partition)

    return records


def transform_record(record: SnapshotRecord) -> SnapshotRecord:
    value = dict(record.value)
    value["payload"] = summarize_payload(value.get("payload"))
    return SnapshotRecord(
        partition=record.partition,
        offset=record.offset,
        key=record.key,
        value=value,
    )


def delete_snapshot_records(
    admin: AdminClient,
    topic: str,
    watermarks: dict[int, tuple[int, int]],
) -> None:
    offsets = [
        TopicPartition(topic, partition, high)
        for partition, (_low, high) in watermarks.items()
        if high > 0
    ]
    if not offsets:
        return
    futures = admin.delete_records(offsets, request_timeout=60, operation_timeout=60)
    for topic_partition, future in futures.items():
        future.result()
        print(f"deleted_before {topic_partition.topic}-{topic_partition.partition}@{topic_partition.offset}")


def publish_records(runtime, topic: str, records: list[SnapshotRecord]) -> None:
    producer = Producer(_kafka_client_config(runtime.kafka.config))
    registry = ProtobufSerializerRegistry(
        schema_registry_url=runtime.kafka.schema_registry_url,
        producer_config=runtime.kafka.config,
        topic_schemas={topic: DLQ_SCHEMA},
        auto_register_schemas=False,
    )
    registry.prepare()

    for record in records:
        payload = registry.serialize(topic, record.value)
        producer.produce(
            topic=topic,
            partition=record.partition,
            key=record.key,
            value=payload,
        )
        producer.poll(0)
    producer.flush()


def write_snapshot_file(path: Path, records: list[SnapshotRecord]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as file:
        for record in records:
            file.write(
                json.dumps(
                    {
                        "partition": record.partition,
                        "offset": record.offset,
                        "key_base64": base64.b64encode(record.key).decode("ascii")
                        if record.key is not None
                        else None,
                        "value": record.value,
                    },
                    ensure_ascii=False,
                    separators=(",", ":"),
                    sort_keys=True,
                )
            )
            file.write("\n")


def read_snapshot_file(path: Path) -> list[SnapshotRecord]:
    records = []
    with path.open("r", encoding="utf-8") as file:
        for line in file:
            if not line.strip():
                continue
            item = json.loads(line)
            key = item.get("key_base64")
            records.append(
                SnapshotRecord(
                    partition=int(item["partition"]),
                    offset=int(item.get("offset", 0)),
                    key=base64.b64decode(key) if key is not None else None,
                    value=item["value"],
                )
            )
    return records


if __name__ == "__main__":
    raise SystemExit(main())
