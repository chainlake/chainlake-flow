#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from confluent_kafka import Producer


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from rpcstream.config.loader import load_pipeline_config  # noqa: E402
from rpcstream.config.resolver import resolve  # noqa: E402
from rpcstream.sinks.kafka.dlq import _kafka_client_config  # noqa: E402
from rpcstream.sinks.kafka.protobuf import DLQ_SCHEMA, ProtobufSerializerRegistry  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Restore unified DLQ records from read_dlq JSONL output."
    )
    parser.add_argument(
        "--config",
        default=str(REPO_ROOT / "rpcstream" / "pipeline.yaml"),
        help="Path to rpcstream pipeline.yaml.",
    )
    parser.add_argument(
        "--input",
        default=str(REPO_ROOT / "scripts" / "dlq_ingestion_examples.jsonl"),
        help="Path to JSONL produced by scripts/read_dlq.py.",
    )
    parser.add_argument("--apply", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    runtime = resolve(load_pipeline_config(str(Path(args.config).resolve())))
    topic = runtime.topic_map.dlq
    if not runtime.kafka.schema_registry_url:
        raise SystemExit("protobuf DLQ restore requires KAFKA_SCHEMA_REGISTRY or KAFAK_SCHEMA_REGISTRY")

    records = load_records(Path(args.input))
    print(
        json.dumps(
            {
                "input": str(Path(args.input).resolve()),
                "topic": topic,
                "records": len(records),
                "dry_run": not args.apply,
            },
            sort_keys=True,
        )
    )
    if not args.apply:
        for record in records[:5]:
            print(json.dumps({"key": record["key"], "value": record["value"]}, sort_keys=True))
        return 0

    producer = Producer(_kafka_client_config(runtime.kafka.config))
    registry = ProtobufSerializerRegistry(
        schema_registry_url=runtime.kafka.schema_registry_url,
        producer_config=runtime.kafka.config,
        topic_schemas={topic: DLQ_SCHEMA},
        auto_register_schemas=False,
    )
    registry.prepare()

    for record in records:
        payload = registry.serialize(topic, record["value"])
        producer.produce(
            topic=topic,
            partition=record.get("partition"),
            key=record["key"],
            value=payload,
        )
        producer.poll(0)
    producer.flush()
    print(json.dumps({"topic": topic, "published": len(records)}, sort_keys=True))
    return 0


def load_records(path: Path) -> list[dict]:
    records = []
    with path.open("r", encoding="utf-8") as file:
        for line_number, line in enumerate(file, start=1):
            if not line.strip():
                continue
            item = json.loads(line)
            key = item.get("key")
            value = item.get("value")
            if not key or not isinstance(value, dict):
                raise SystemExit(f"invalid record at {path}:{line_number}")
            records.append(
                {
                    "key": key,
                    "partition": item.get("partition"),
                    "value": normalize_value(value),
                }
            )
    return records


def normalize_value(value: dict) -> dict:
    normalized = dict(value)
    if "payload" not in normalized:
        normalized["payload"] = {
            "type": normalized.pop("payload_type", "unknown"),
            "size": int(normalized.pop("payload_size", 0) or 0),
        }
    else:
        normalized.pop("payload_type", None)
        normalized.pop("payload_size", None)

    if "context" not in normalized:
        context_keys = normalized.pop("context_keys", [])
        normalized["context"] = {"keys": context_keys}
    else:
        normalized.pop("context_keys", None)

    for field in (
        "block_number",
        "retry_count",
        "max_retry",
        "first_seen_at",
        "last_attempt_at",
        "ingest_timestamp",
    ):
        normalized[field] = int(normalized.get(field) or 0)

    next_retry_at = normalized.get("next_retry_at")
    normalized["next_retry_at"] = int(next_retry_at) if next_retry_at is not None else None
    return normalized


if __name__ == "__main__":
    raise SystemExit(main())
