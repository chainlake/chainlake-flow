from __future__ import annotations

import argparse
import asyncio
import os

from rpcstream.app_runtime import build_runtime_stack
from rpcstream.planner.dlq_replay import DlqReplayBlockSource
from rpcstream.sinks.kafka.dlq import UnifiedDlqKafkaClient

DEFAULT_REPLAY_GROUP = "rpcstream-dlq-replay"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Replay blocks from unified DLQ records")
    parser.add_argument("--entity", default=None)
    parser.add_argument("--status", default="failed")
    parser.add_argument("--stage", default=None)
    parser.add_argument("--max-records", type=int, default=None)
    return parser


async def run(args: argparse.Namespace) -> None:
    config_path = os.getenv("PIPELINE_CONFIG", "pipeline.yaml")
    group_id = os.getenv("DLQ_REPLAY_GROUP_ID", DEFAULT_REPLAY_GROUP)

    stack = build_runtime_stack(config_path=config_path, with_tracker=False)
    dlq_client = UnifiedDlqKafkaClient(
        topic=stack.runtime.topic_map.dlq,
        producer_config=stack.runtime.kafka.config,
        schema_registry_url=stack.runtime.kafka.schema_registry_url,
        group_id=group_id,
        logger=stack.logger,
    )
    source = DlqReplayBlockSource(
        dlq_client,
        entity=args.entity,
        status=args.status,
        stage=args.stage,
        pipeline=stack.runtime.pipeline.name,
        chain=stack.runtime.chain.type,
        max_records=args.max_records,
        logger=stack.logger,
    )

    await stack.start()
    try:
        stack.logger.info(
            "dlq.replay_started",
            component="dlq",
            topic=stack.runtime.topic_map.dlq,
            entity=args.entity,
            status=args.status,
            stage=args.stage,
            max_records=args.max_records,
        )
        await stack.engine.run_stream(source)
    finally:
        dlq_client.close()
        await stack.close()


def cli() -> None:
    args = build_parser().parse_args()
    asyncio.run(run(args))


if __name__ == "__main__":
    cli()
