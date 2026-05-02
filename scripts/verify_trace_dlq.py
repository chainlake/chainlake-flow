#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
import uuid
from pathlib import Path
from types import SimpleNamespace

from confluent_kafka import Consumer, KafkaError, Producer


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from rpcstream.config.loader import load_pipeline_config  # noqa: E402
from rpcstream.config.resolver import resolve  # noqa: E402
from rpcstream.ingestion.engine import IngestionEngine  # noqa: E402
from rpcstream.runtime.observability.context import ObservabilityContext  # noqa: E402
from rpcstream.sinks.kafka.bootstrap import build_protobuf_topic_schemas  # noqa: E402
from rpcstream.sinks.kafka.dlq import UnifiedDlqKafkaClient  # noqa: E402
from rpcstream.sinks.kafka.producer import KafkaWriter  # noqa: E402
from rpcstream.utils.logger import JsonLogger  # noqa: E402
from rpcstream.adapters.evm.identity.event_id_calculator import EventIdCalculator  # noqa: E402
from rpcstream.adapters.evm.identity.event_time_calculator import EventTimeCalculator  # noqa: E402
from rpcstream.adapters.evm.enrich import EvmEnricher  # noqa: E402


class DummyFetcher:
    def __init__(self, value, meta):
        self.value = value
        self.meta = meta

    async def fetch(self, _block_number):
        return {"trace": (self.value, self.meta)}


class FailingTraceProcessor:
    def process(self, _block_number, _value):
        raise TypeError("list indices must be integers or slices, not str")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Trigger one synthetic trace processor failure and verify it lands in dlq.ingestion."
    )
    parser.add_argument(
        "--config",
        default=str(REPO_ROOT / "rpcstream" / "pipeline.yaml"),
        help="Path to rpcstream pipeline.yaml.",
    )
    parser.add_argument(
        "--block-number",
        type=int,
        default=int(time.time()),
        help="Synthetic block number to tag the DLQ record with.",
    )
    parser.add_argument(
        "--timeout-sec",
        type=float,
        default=20.0,
        help="How long to wait for the verification read.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    config_path = Path(args.config).resolve()
    runtime = resolve(load_pipeline_config(str(config_path)))
    logger = JsonLogger(level="info")

    marker = f"trace-dlq-verify-{uuid.uuid4().hex[:12]}"
    consumer = build_verifier_consumer(runtime, marker)
    try:
        consumer.subscribe()
        consumer.wait_until_ready(timeout_sec=10.0)

        asyncio.run(send_one_trace_dlq(runtime, logger, marker, args.block_number))
        record = poll_for_record(
            runtime=runtime,
            consumer=consumer,
            marker=marker,
            block_number=args.block_number,
            timeout_sec=args.timeout_sec,
        )
    finally:
        consumer.close()

    print(
        json.dumps(
            {
                "verified": True,
                "topic": runtime.topic_map.dlq,
                "entity": record["entity"],
                "block_number": record["block_number"],
                "status": record["status"],
                "error_type": record["error_type"],
                "error_message": record["error_message"],
                "marker": marker,
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
    )
    return 0


async def send_one_trace_dlq(runtime, logger, marker: str, block_number: int) -> None:
    writer = KafkaWriter(
        producer=Producer(runtime.kafka.config),
        id_calculator=EventIdCalculator(),
        time_calculator=EventTimeCalculator(),
        logger=logger,
        config=runtime.kafka.streaming,
        producer_config=runtime.kafka.config,
        topic_maps=runtime.topic_map,
        protobuf_enabled=runtime.kafka.protobuf_enabled,
        schema_registry_url=runtime.kafka.schema_registry_url,
        protobuf_topic_schemas=build_protobuf_topic_schemas(runtime.topic_map, runtime.entities),
        observability=ObservabilityContext.disabled(),
        eos_enabled=runtime.kafka.eos_enabled,
        eos_init_timeout_sec=runtime.kafka.eos_init_timeout_sec,
    )

    fetch_meta = SimpleNamespace(extra={"marker": marker, "script": "verify_trace_dlq"})
    engine = IngestionEngine(
        fetcher=DummyFetcher(value=[], meta=fetch_meta),
        processors={"trace": FailingTraceProcessor()},
        enricher=EvmEnricher(),
        sink=writer,
        topics=runtime.topic_map.main,
        dlq_topic=runtime.topic_map.dlq,
        chain=runtime.chain,
        pipeline=runtime.pipeline,
        max_retry=runtime.client.max_retries,
        concurrency=1,
        logger=logger,
        observability=ObservabilityContext.disabled(),
        checkpoint_reader=None,
        eos_enabled=runtime.kafka.eos_enabled,
    )

    await writer.start()
    try:
        success, _delivery_futures, _expected_watermark = await engine._run_one(block_number)
        if success:
            raise RuntimeError("expected synthetic trace processor failure, but block succeeded")
    finally:
        await writer.close()


def build_verifier_consumer(runtime, marker: str):
    group_id = f"rpcstream-dlq-verify-{marker}"
    if runtime.kafka.protobuf_enabled:
        if not runtime.kafka.schema_registry_url:
            raise SystemExit(
                "protobuf DLQ verification requires KAFKA_SCHEMA_REGISTRY or KAFAK_SCHEMA_REGISTRY"
            )
        return ProtobufDlqVerifier(
            UnifiedDlqKafkaClient(
                topic=runtime.topic_map.dlq,
                producer_config=runtime.kafka.config,
                schema_registry_url=runtime.kafka.schema_registry_url,
                group_id=group_id,
                auto_offset_reset="latest",
            )
        )

    return JsonDlqVerifier(
        topic=runtime.topic_map.dlq,
        producer_config=runtime.kafka.config,
        group_id=group_id,
    )


def poll_for_record(runtime, consumer, marker: str, block_number: int, timeout_sec: float) -> dict:
    deadline = time.monotonic() + timeout_sec
    while time.monotonic() < deadline:
        message = consumer.poll(timeout=1.0)
        if message is None:
            continue

        record = message["value"]
        context = record.get("context") or {}
        if (
            record.get("entity") == "trace"
            and record.get("block_number") == block_number
            and context.get("meta", {}).get("marker") == marker
        ):
            return record

    raise SystemExit(
        f"timed out after {timeout_sec}s waiting for marker={marker} in topic={runtime.topic_map.dlq}"
    )


class ProtobufDlqVerifier:
    def __init__(self, client: UnifiedDlqKafkaClient):
        self.client = client

    def subscribe(self) -> None:
        self.client.subscribe()

    def wait_until_ready(self, timeout_sec: float) -> None:
        deadline = time.monotonic() + timeout_sec
        while time.monotonic() < deadline:
            self.client.poll(timeout=0.2)
            if self.client._consumer.assignment():
                return
        raise SystemExit(f"timed out after {timeout_sec}s waiting for DLQ consumer assignment")

    def poll(self, timeout: float = 1.0):
        message = self.client.poll(timeout=timeout)
        if message is None:
            return None
        return {
            "topic": message.topic,
            "partition": message.partition,
            "offset": message.offset,
            "key": message.key,
            "value": message.value,
        }

    def close(self) -> None:
        self.client.close()


class JsonDlqVerifier:
    def __init__(self, *, topic: str, producer_config: dict, group_id: str):
        self.topic = topic
        self.consumer = Consumer(
            {
                **json_consumer_config(producer_config),
                "group.id": group_id,
                "auto.offset.reset": "latest",
                "enable.auto.commit": False,
                "isolation.level": "read_committed",
            }
        )

    def subscribe(self) -> None:
        self.consumer.subscribe([self.topic])

    def wait_until_ready(self, timeout_sec: float) -> None:
        deadline = time.monotonic() + timeout_sec
        while time.monotonic() < deadline:
            self.consumer.poll(0.2)
            if self.consumer.assignment():
                return
        raise SystemExit(f"timed out after {timeout_sec}s waiting for DLQ consumer assignment")

    def poll(self, timeout: float = 1.0):
        message = self.consumer.poll(timeout)
        if message is None:
            return None
        if message.error():
            if message.error().code() == KafkaError._PARTITION_EOF:
                return None
            raise RuntimeError(str(message.error()))

        key = message.key().decode("utf-8") if message.key() else None
        value = json.loads(message.value().decode("utf-8"))
        return {
            "topic": message.topic(),
            "partition": message.partition(),
            "offset": message.offset(),
            "key": key,
            "value": value,
        }

    def close(self) -> None:
        self.consumer.close()


def json_consumer_config(producer_config: dict) -> dict:
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


if __name__ == "__main__":
    raise SystemExit(main())
