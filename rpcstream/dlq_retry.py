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
        with_checkpoint=True,
    )
    client = UnifiedDlqKafkaClient(
        topic=stack.runtime.topic_map.dlq,
        producer_config=stack.runtime.kafka.config,
        schema_registry_url=stack.runtime.kafka.schema_registry_url,
        group_id=group_id,
        logger=stack.logger,
    )

    await stack.start()
    await stack.engine.sink.start()
    client.subscribe()

    stack.logger.info(
        "dlq.retry_worker_started",
        component="dlq",
        topic=stack.runtime.topic_map.dlq,
        group_id=group_id,
    )

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

            delay_ms = retry_delay_ms(record)
            if delay_ms > 0:
                await asyncio.sleep(delay_ms / 1000.0)

            success = await stack.engine.retry_dlq_record(record)
            if success:
                await stack.engine.mark_dlq_resolved(record)
                stack.logger.info(
                    "dlq.retry_succeeded",
                    component="dlq",
                    entity=record.get("entity"),
                    block_number=record.get("block_number"),
                    retry_count=record.get("retry_count", 0),
                )
            else:
                stack.logger.warn(
                    "dlq.retry_failed",
                    component="dlq",
                    entity=record.get("entity"),
                    block_number=record.get("block_number"),
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
