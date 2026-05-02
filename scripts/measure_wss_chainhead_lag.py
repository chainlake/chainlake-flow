from __future__ import annotations

import argparse
import asyncio
import socket
import time
from dataclasses import dataclass, field
from contextlib import suppress
from pathlib import Path
from typing import Any

import aiohttp
import orjson

from rpcstream.config.builder import build_erpc_endpoint
from rpcstream.config.loader import load_pipeline_config
from rpcstream.config.resolver import resolve


def _now_ms() -> int:
    return int(time.time() * 1000)


def _format_ms(value: float | int | None) -> str:
    if value is None:
        return "n/a"
    return f"{float(value):.1f}ms"


def _load_runtime(config_path: str):
    raw_config = load_pipeline_config(config_path)
    runtime = resolve(raw_config)
    return raw_config, runtime


def _build_ws_url(raw_config, override: str | None = None) -> str:
    websocket_url = (override or getattr(raw_config.tracker, "websocket_url", None) or "").strip()
    if not websocket_url:
        raise ValueError(
            "tracker.websocket_url is missing. Set it in pipeline.yaml or pass --websocket-url."
        )
    if not websocket_url.startswith(("ws://", "wss://")):
        raise ValueError("websocket url must start with ws:// or wss://")
    return websocket_url


def _build_http_url(raw_config) -> str:
    return build_erpc_endpoint(raw_config)


def _build_rpc_payload(method: str, params: list[Any] | None = None) -> dict[str, Any]:
    return {
        "jsonrpc": "2.0",
        "id": str(time.time_ns()),
        "method": method,
        "params": params or [],
    }


def _hex_to_int(value: str | None) -> int | None:
    if not isinstance(value, str):
        return None
    try:
        return int(value, 16)
    except ValueError:
        return None


def _extract_block_number(payload: dict[str, Any]) -> int | None:
    if payload.get("method") != "eth_subscription":
        return None
    params = payload.get("params") or {}
    result = params.get("result") or {}
    if not isinstance(result, dict):
        return None
    return _hex_to_int(result.get("number"))


async def _rpc_call(session: aiohttp.ClientSession, base_url: str, method: str, params: list[Any] | None = None):
    async with session.post(base_url, json=_build_rpc_payload(method, params)) as resp:
        resp.raise_for_status()
        raw = await resp.read()
    payload = orjson.loads(raw)
    if "error" in payload:
        raise RuntimeError(f"rpc error calling {method}: {payload['error']}")
    return payload["result"]


async def _http_block_number(session: aiohttp.ClientSession, base_url: str) -> int:
    result = await _rpc_call(session, base_url, "eth_blockNumber")
    if not isinstance(result, str):
        raise RuntimeError(f"unexpected eth_blockNumber result: {result!r}")
    return int(result, 16)


async def _http_block_header(session: aiohttp.ClientSession, base_url: str, block_number: int) -> dict[str, Any]:
    result = await _rpc_call(session, base_url, "eth_getBlockByNumber", [hex(block_number), False])
    if not isinstance(result, dict):
        raise RuntimeError(f"unexpected eth_getBlockByNumber result: {result!r}")
    return result


@dataclass
class HeadSample:
    block_number: int
    ws_seen_at_ms: int | None = None
    poll_seen_at_ms: int | None = None
    block_timestamp_ms: int | None = None
    ws_after_block_ts_ms: float | None = None
    poll_after_block_ts_ms: float | None = None

    def as_row(self) -> list[str]:
        ws_vs_poll = None
        if self.ws_seen_at_ms is not None and self.poll_seen_at_ms is not None:
            ws_vs_poll = float(self.poll_seen_at_ms - self.ws_seen_at_ms)
        return [
            str(self.block_number),
            str(self.ws_seen_at_ms) if self.ws_seen_at_ms is not None else "n/a",
            str(self.poll_seen_at_ms) if self.poll_seen_at_ms is not None else "n/a",
            _format_ms(ws_vs_poll),
            _format_ms(self.block_timestamp_ms),
            _format_ms(self.ws_after_block_ts_ms),
            _format_ms(self.poll_after_block_ts_ms),
        ]


def _print_table(samples: list[HeadSample]) -> None:
    headers = [
        "block",
        "ws_seen_at_ms",
        "poll_seen_at_ms",
        "ws_minus_poll",
        "block_timestamp_ms",
        "ws_after_block_ts",
        "poll_after_block_ts",
    ]
    widths = [len(header) for header in headers]
    rows = [sample.as_row() for sample in samples]
    for row in rows:
        for idx, value in enumerate(row):
            widths[idx] = max(widths[idx], len(value))

    def render_row(values: list[str]) -> str:
        return "  ".join(value.ljust(widths[idx]) for idx, value in enumerate(values))

    print(render_row(headers))
    print("  ".join("-" * width for width in widths))
    for row in rows:
        print(render_row(row))


async def measure(
    *,
    config_path: str,
    samples: int,
    poll_interval_sec: float,
    websocket_url: str | None,
    block_timeout_sec: float,
) -> list[HeadSample]:
    raw_config, _runtime = _load_runtime(config_path)
    http_url = _build_http_url(raw_config)
    ws_url = _build_ws_url(raw_config, override=websocket_url)

    connector = aiohttp.TCPConnector(limit=20, ttl_dns_cache=300, keepalive_timeout=30)
    timeout = aiohttp.ClientTimeout(total=block_timeout_sec)
    samples_by_block: dict[int, HeadSample] = {}
    ws_seen_order: list[int] = []
    stop_event = asyncio.Event()
    subscription_ready = asyncio.Event()
    current_poll_head: int | None = None

    async with aiohttp.ClientSession(
        connector=connector,
        timeout=timeout,
        json_serialize=lambda obj: orjson.dumps(obj).decode(),
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Accept-Encoding": "gzip, deflate",
        },
    ) as session:
        async def ws_listener() -> None:
            nonlocal current_poll_head
            async with session.ws_connect(ws_url, heartbeat=30) as ws:
                await ws.send_json(
                    {
                        "jsonrpc": "2.0",
                        "id": "1",
                        "method": "eth_subscribe",
                        "params": ["newHeads"],
                    }
                )
                while not stop_event.is_set():
                    msg = await ws.receive()
                    if msg.type in {aiohttp.WSMsgType.TEXT, aiohttp.WSMsgType.BINARY}:
                        payload = orjson.loads(msg.data if isinstance(msg.data, (bytes, bytearray)) else str(msg.data).encode())
                        if isinstance(payload, dict) and payload.get("id") == "1" and "result" in payload:
                            subscription_ready.set()
                            continue
                        block_number = _extract_block_number(payload)
                        if block_number is None:
                            continue
                        seen_at_ms = _now_ms()
                        sample = samples_by_block.setdefault(block_number, HeadSample(block_number=block_number))
                        if sample.ws_seen_at_ms is None:
                            sample.ws_seen_at_ms = seen_at_ms
                            ws_seen_order.append(block_number)
                            if sample.block_timestamp_ms is not None:
                                sample.ws_after_block_ts_ms = float(seen_at_ms - sample.block_timestamp_ms)
                            if current_poll_head == block_number and sample.poll_seen_at_ms is None:
                                sample.poll_seen_at_ms = seen_at_ms
                                if sample.block_timestamp_ms is not None:
                                    sample.poll_after_block_ts_ms = float(seen_at_ms - sample.block_timestamp_ms)
                            if len(ws_seen_order) >= samples:
                                stop_event.set()
                                return
                    elif msg.type in {aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.CLOSING, aiohttp.WSMsgType.ERROR}:
                        raise ConnectionError(f"websocket closed with type={msg.type!s}")

        async def poller() -> None:
            nonlocal current_poll_head
            await subscription_ready.wait()
            while not stop_event.is_set():
                try:
                    latest_head = await _http_block_number(session, http_url)
                    current_poll_head = latest_head
                    seen_at_ms = _now_ms()
                    sample = samples_by_block.setdefault(latest_head, HeadSample(block_number=latest_head))
                    if sample.poll_seen_at_ms is None:
                        sample.poll_seen_at_ms = seen_at_ms
                        if sample.block_timestamp_ms is not None:
                            sample.poll_after_block_ts_ms = float(seen_at_ms - sample.block_timestamp_ms)
                    await asyncio.sleep(poll_interval_sec)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    await asyncio.sleep(min(poll_interval_sec, 0.5))

        async def enrich_block_headers() -> None:
            while not stop_event.is_set() or len(ws_seen_order) < samples:
                pending = [block for block in ws_seen_order if samples_by_block.get(block) and samples_by_block[block].block_timestamp_ms is None]
                if not pending:
                    await asyncio.sleep(0.1)
                    continue
                for block_number in pending[:10]:
                    try:
                        header = await _http_block_header(session, http_url, block_number)
                        timestamp_hex = header.get("timestamp")
                        block_timestamp_ms = int(timestamp_hex, 16) * 1000 if isinstance(timestamp_hex, str) else None
                        sample = samples_by_block[block_number]
                        sample.block_timestamp_ms = block_timestamp_ms
                        if sample.ws_seen_at_ms is not None and block_timestamp_ms is not None:
                            sample.ws_after_block_ts_ms = float(sample.ws_seen_at_ms - block_timestamp_ms)
                        if sample.poll_seen_at_ms is not None and block_timestamp_ms is not None:
                            sample.poll_after_block_ts_ms = float(sample.poll_seen_at_ms - block_timestamp_ms)
                    except Exception:
                        continue
                await asyncio.sleep(0.1)

        tasks = [
            asyncio.create_task(ws_listener()),
            asyncio.create_task(poller()),
            asyncio.create_task(enrich_block_headers()),
        ]
        try:
            await stop_event.wait()
        finally:
            for task in tasks:
                task.cancel()
            for task in tasks:
                with suppress(asyncio.CancelledError, Exception):
                    await task

    ordered_blocks = ws_seen_order[:samples]
    return [samples_by_block[block] for block in ordered_blocks if block in samples_by_block]


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Measure websocket chainhead lag versus HTTP polling for a pipeline config.",
    )
    parser.add_argument(
        "--config",
        default="pipeline.yaml",
        help="Path to pipeline.yaml.",
    )
    parser.add_argument(
        "--samples",
        type=int,
        default=20,
        help="How many websocket head updates to capture.",
    )
    parser.add_argument(
        "--poll-interval",
        type=float,
        default=0.5,
        help="HTTP poll interval in seconds for the comparison poller.",
    )
    parser.add_argument(
        "--websocket-url",
        default=None,
        help="Override tracker.websocket_url from pipeline.yaml.",
    )
    parser.add_argument(
        "--block-timeout-sec",
        type=float,
        default=30.0,
        help="Timeout for each HTTP RPC request.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print JSON instead of a table.",
    )
    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

    samples = asyncio.run(
        measure(
            config_path=args.config,
            samples=max(1, args.samples),
            poll_interval_sec=max(0.05, args.poll_interval),
            websocket_url=args.websocket_url,
            block_timeout_sec=max(1.0, args.block_timeout_sec),
        )
    )

    if args.json:
        print(
            orjson.dumps(
                {
                    "host": socket.gethostname(),
                    "config": args.config,
                    "samples": [
                        {
                            "block_number": sample.block_number,
                            "ws_seen_at_ms": sample.ws_seen_at_ms,
                            "poll_seen_at_ms": sample.poll_seen_at_ms,
                            "ws_minus_poll_ms": (
                                sample.poll_seen_at_ms - sample.ws_seen_at_ms
                                if sample.ws_seen_at_ms is not None and sample.poll_seen_at_ms is not None
                                else None
                            ),
                            "block_timestamp_ms": sample.block_timestamp_ms,
                            "ws_after_block_ts_ms": sample.ws_after_block_ts_ms,
                            "poll_after_block_ts_ms": sample.poll_after_block_ts_ms,
                        }
                        for sample in samples
                    ],
                },
                option=orjson.OPT_INDENT_2,
            ).decode()
        )
        return 0

    print(f"host={socket.gethostname()} config={args.config}")
    _print_table(samples)
    print()
    print("Notes:")
    print("- ws_minus_poll_ms is the direct websocket advantage over the HTTP poller.")
    print("- ws_after_block_ts_ms and poll_after_block_ts_ms are approximate ages relative to the block timestamp.")
    print("- If ws_minus_poll_ms is small, the websocket provider is probably not pushing much earlier than the poll interval.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
