from __future__ import annotations

import asyncio
import os

from rpcstream.app_runtime import build_runtime_stack
from rpcstream.ingestion.dlq import retry_delay_ms, should_retry_record
from rpcstream.sinks.kafka.dlq import UnifiedDlqKafkaClient

DEFAULT_RETRY_GROUP = "rpcstream-dlq-retry"


async def run_dlq_retry(
    *,
    config_path: str | None = None,
    config=None,
    group_id: str | None = None,
) -> None:
    config_path = config_path or os.getenv("PIPELINE_CONFIG", "pipeline.yaml")
    group_id = group_id or os.getenv("DLQ_RETRY_GROUP_ID", DEFAULT_RETRY_GROUP)

    stack = build_runtime_stack(
        config_path=config_path,
        config=config,
        with_tracker=False,
    )
    client = UnifiedDlqKafkaClient(
        topic=stack.runtime.topic_map.dlq,
        producer_config=stack.runtime.kafka.config,
        schema_registry_url=stack.runtime.kafka.schema_registry_url,
        schema_registry_type=stack.runtime.kafka.schema_registry_type or "protobuf",
        group_id=group_id,
        logger=stack.logger,
    )

    await stack.start()
    await stack.engine.sink.start()
    client.subscribe()

    stack.logger.info(
        "dlq.retry_worker_started",
        topic=stack.runtime.topic_map.dlq,
        group_id=group_id,
    )

    # A cursor can have multiple DLQ records (one per entity that failed --
    # see engine._send_sink_failure_dlq's history) queued back when a single
    # sink hiccup timed out several entities' deliveries at once. Each record
    # independently triggers a full cursor reprocess+resink on retry, so
    # replaying every one of them for an already-succeeded cursor would keep
    # producing duplicate Kafka messages for as long as the backlog takes to
    # drain. Once a cursor has succeeded in this process, later records for
    # the same cursor are resolved without reprocessing.
    resolved_cursors: set[int] = set()

    try:
        while True:
            message = await asyncio.to_thread(client.poll, 1.0)
            if message is None:
                await asyncio.sleep(0.1)
                continue

            record = message.value
            if not should_retry_record(record):
                client.commit(message)
                continue

            cursor = record.get("cursor")
            if cursor in resolved_cursors:
                await stack.engine.mark_dlq_resolved(record)
                stack.logger.info(
                    "dlq.retry_skipped_already_resolved",
                    entity=record.get("entity"),
                    cursor=cursor,
                )
                client.commit(message)
                continue

            delay_ms = retry_delay_ms(record)
            if delay_ms > 0:
                await asyncio.sleep(delay_ms / 1000.0)

            success = await stack.engine.retry_dlq_record(record)
            if success:
                resolved_cursors.add(cursor)
                await stack.engine.mark_dlq_resolved(record)
                stack.logger.info(
                    "dlq.retry_succeeded",
                    entity=record.get("entity"),
                    cursor=record.get("cursor"),
                    retry_count=record.get("retry_count", 0),
                )
            else:
                stack.logger.warn(
                    "dlq.retry_failed",
                    entity=record.get("entity"),
                    cursor=record.get("cursor"),
                    retry_count=record.get("retry_count", 0) + 1,
                )

            client.commit(message)
    finally:
        client.close()
        await stack.engine.sink.close()
        await stack.close()


async def main() -> None:
    await run_dlq_retry()


def cli() -> None:
    asyncio.run(main())


if __name__ == "__main__":
    cli()
