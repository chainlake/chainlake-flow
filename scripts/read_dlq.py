#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from rpcstream.config.loader import load_pipeline_config  # noqa: E402
from rpcstream.config.resolver import resolve  # noqa: E402
from rpcstream.ingestion.dlq import matches_replay_filter  # noqa: E402
from rpcstream.sinks.kafka.dlq import UnifiedDlqKafkaClient  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Read and decode protobuf records from the unified DLQ topic."
    )
    parser.add_argument(
        "--config",
        default=str(REPO_ROOT / "rpcstream" / "pipeline.yaml"),
        help="Path to rpcstream pipeline.yaml.",
    )
    parser.add_argument(
        "--group-id",
        default=f"rpcstream-dlq-reader-{int(time.time())}",
        help="Kafka consumer group id. Defaults to a one-off group.",
    )
    parser.add_argument(
        "--offset",
        choices=("earliest", "latest"),
        default="earliest",
        help="Starting offset for a new consumer group.",
    )
    parser.add_argument("--max-records", type=int, default=20)
    parser.add_argument("--timeout-sec", type=float, default=10.0)
    parser.add_argument("--entity")
    parser.add_argument("--status")
    parser.add_argument("--stage")
    parser.add_argument("--pipeline")
    parser.add_argument("--chain")
    parser.add_argument(
        "--pretty",
        action="store_true",
        help="Pretty-print JSON instead of newline-delimited compact JSON.",
    )
    parser.add_argument(
        "--summary",
        action="store_true",
        help="Print metadata and error fields without the full payload/context.",
    )
    parser.add_argument(
        "--commit",
        action="store_true",
        help="Commit consumed offsets for the chosen group id.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    config_path = Path(args.config).resolve()
    runtime = resolve(load_pipeline_config(str(config_path)))

    if not runtime.kafka.schema_registry_url:
        raise SystemExit("protobuf DLQ decoding requires KAFKA_SCHEMA_REGISTRY or KAFAK_SCHEMA_REGISTRY")

    client = UnifiedDlqKafkaClient(
        topic=runtime.topic_map.dlq,
        producer_config=runtime.kafka.config,
        schema_registry_url=runtime.kafka.schema_registry_url,
        group_id=args.group_id,
        auto_offset_reset=args.offset,
    )

    print(
        json.dumps(
            {
                "topic": runtime.topic_map.dlq,
                "group_id": args.group_id,
                "offset": args.offset,
                "config": str(config_path),
            },
            sort_keys=True,
        ),
        file=sys.stderr,
    )

    client.subscribe()
    deadline = time.monotonic() + args.timeout_sec
    emitted = 0
    scanned = 0
    try:
        while emitted < args.max_records and time.monotonic() < deadline:
            message = client.poll(timeout=1.0)
            if message is None:
                continue
            scanned += 1
            record = message.value
            if not matches_replay_filter(
                record,
                entity=args.entity,
                status=args.status,
                stage=args.stage,
                pipeline=args.pipeline,
                chain=args.chain,
            ):
                if args.commit:
                    client.commit(message)
                continue

            value = summarize_record(record) if args.summary else record
            output = {
                "topic": message.topic,
                "partition": message.partition,
                "offset": message.offset,
                "key": message.key,
                "value": value,
            }
            if args.pretty:
                print(json.dumps(output, ensure_ascii=False, indent=2, sort_keys=True))
            else:
                print(json.dumps(output, ensure_ascii=False, separators=(",", ":"), sort_keys=True))

            emitted += 1
            if args.commit:
                client.commit(message)
    finally:
        client.close()

    print(
        json.dumps(
            {"scanned": scanned, "emitted": emitted},
            sort_keys=True,
        ),
        file=sys.stderr,
    )
    return 0


def summarize_record(record: dict) -> dict:
    fields = (
        "id",
        "chain",
        "network",
        "pipeline",
        "entity",
        "block_number",
        "stage",
        "error_type",
        "error_message",
        "status",
        "retry_count",
        "max_retry",
        "first_seen_at",
        "last_attempt_at",
        "next_retry_at",
        "ingest_timestamp",
    )
    summary = {field: record.get(field) for field in fields}
    payload = record.get("payload")
    context = record.get("context")
    if is_payload_summary(payload):
        summary["payload"] = payload
    else:
        summary["payload_type"] = type(payload).__name__
        summary["payload_size"] = len(payload) if hasattr(payload, "__len__") else None
        if isinstance(payload, dict):
            summary["payload"] = payload
    summary["context_keys"] = sorted(context.keys()) if isinstance(context, dict) else []
    return summary


def is_payload_summary(payload) -> bool:
    return (
        isinstance(payload, dict)
        and "type" in payload
        and set(payload).issubset({"type", "size", "preview"})
    )


if __name__ == "__main__":
    raise SystemExit(main())
