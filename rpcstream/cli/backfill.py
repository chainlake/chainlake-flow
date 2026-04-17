import argparse
import asyncio

from cli.logs import run


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Ethereum-style backfill logs CLI")
    parser.add_argument("--start-block", type=int, required=True)
    parser.add_argument("--end-block", type=int, required=True)
    parser.add_argument("--range-size", type=int, default=100)
    parser.add_argument("--erpc-url", default=None)
    parser.add_argument("--rpc-timeout", type=int, default=None)
    parser.add_argument("--max-inflight-rpc", type=int, default=None)
    parser.add_argument("--max-inflight-ranges", type=int, default=20)
    parser.add_argument("--poll-interval", type=float, default=None)
    parser.add_argument("--chain", default="evm")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
