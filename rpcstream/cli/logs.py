import argparse
import asyncio
from typing import Any

from blockchain_ingestion.config.settings import load_settings
from blockchain_ingestion.planner.range_planner import BoundedRangePlanner, TailingRangePlanner
from blockchain_ingestion.planner.stream_cursor import LatestBlockTracker
from blockchain_ingestion.rpc.erpc_client import ErpcClient, ErpcScheduler
from blockchain_ingestion.runtime.engine import IngestionEngine
from blockchain_ingestion.utils.logging import get_logger

logger = get_logger(__name__)


async def submit_logs_range(scheduler: ErpcScheduler, registry, r):
    meta = {
        "range_id": r.range_id,
        "start_block": r.start_block,
        "end_block": r.end_block,
        "retry": r.retry,
    }
    task = asyncio.create_task(
        scheduler.submit(
            "eth_getLogs",
            [{"fromBlock": hex(r.start_block), "toBlock": hex(r.end_block)}],
            meta,
        )
    )
    registry.mark_inflight(r.range_id, task_id=id(task))
    return task


async def on_commit_logs(result) -> None:
    payload = result.payload or []
    log_count = len(payload)
    logger.info(
        "range committed",
        extra={
            "range_id": result.range_id,
            "start": result.start_block,
            "end": result.end_block,
            "logs": log_count,
        },
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Ethereum-style logs ingest CLI")
    parser.add_argument("--chain", default="evm")
    parser.add_argument("--start-block", type=int, required=True)
    parser.add_argument("--end-block", type=int)
    parser.add_argument("--range-size", type=int, default=10)
    parser.add_argument("--erpc-url", default=None)
    parser.add_argument("--rpc-timeout", type=int, default=None)
    parser.add_argument("--max-inflight-rpc", type=int, default=None)
    parser.add_argument("--max-inflight-ranges", type=int, default=20)
    parser.add_argument("--poll-interval", type=float, default=None)
    return parser


async def run(args: argparse.Namespace) -> None:
    settings = load_settings()
    erpc_url = args.erpc_url or settings.erpc_url
    rpc_timeout = args.rpc_timeout or settings.rpc_timeout_sec
    max_inflight_rpc = args.max_inflight_rpc or settings.max_inflight_rpc
    poll_interval = args.poll_interval or settings.poll_interval_sec

    client = ErpcClient(base_url=erpc_url, timeout_sec=rpc_timeout)
    scheduler = ErpcScheduler(client=client, max_inflight=max_inflight_rpc)
    tracker = LatestBlockTracker(client=client, refresh_interval=poll_interval)

    if args.end_block is not None:
        planner = BoundedRangePlanner(args.start_block, args.end_block, args.range_size)
    else:
        planner = TailingRangePlanner(args.start_block, args.range_size)

    engine = IngestionEngine(
        planner=planner,
        scheduler=scheduler,
        tracker=tracker,
        submit_range_fn=submit_logs_range,
        on_commit=on_commit_logs,
        max_inflight_ranges=args.max_inflight_ranges,
    )
    await engine.run()


def main() -> None:
    args = build_parser().parse_args()
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
