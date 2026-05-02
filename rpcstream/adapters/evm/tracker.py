from __future__ import annotations

import asyncio
import time
from contextlib import suppress

import aiohttp
import orjson

from rpcstream.adapters.evm.rpc_requests import build_eth_blockNumber


class EvmChainHeadTracker:
    def __init__(
        self,
        client,
        poll_interval=0.2,
        websocket_url: str | None = None,
        logger=None,
        websocket_reconnect_delay_sec: float = 1.0,
    ):
        self.client = client
        self.poll_interval = poll_interval
        self.websocket_url = websocket_url.strip() if websocket_url else None
        self.logger = logger
        self.websocket_reconnect_delay_sec = websocket_reconnect_delay_sec

        self._head_cursor = None
        self._running = False
        self._poll_task: asyncio.Task | None = None
        self._ws_task: asyncio.Task | None = None
        self._ws_session: aiohttp.ClientSession | None = None

        self._last_poll_started_at_ms = None
        self._last_poll_completed_at_ms = None
        self._last_poll_latency_ms = None
        self._last_update_ts = 0
        self._last_head_source = None

    def get_head_cursor(self):
        return self._head_cursor

    def get_latest(self):
        return self.get_head_cursor()

    def get_last_update_at_ms(self):
        return self._last_update_ts * 1000 if self._last_update_ts else None

    def get_last_poll_started_at_ms(self):
        return self._last_poll_started_at_ms

    def get_last_poll_completed_at_ms(self):
        return self._last_poll_completed_at_ms

    def get_last_observation_latency_ms(self):
        return self._last_poll_latency_ms

    def get_last_head_source(self):
        return self._last_head_source

    def get_lag(self, current_cursor):
        if self._head_cursor is None:
            return None
        return self._head_cursor - current_cursor

    async def start(self):
        if self._running:
            return
        self._running = True
        self._poll_task = asyncio.create_task(self._run_poll())
        if self.websocket_url:
            self._ws_task = asyncio.create_task(self._run_websocket())

    async def stop(self):
        self._running = False

        if self._ws_session is not None and not self._ws_session.closed:
            await self._ws_session.close()

        tasks = [task for task in (self._poll_task, self._ws_task) if task is not None]
        for task in tasks:
            task.cancel()
        for task in tasks:
            with suppress(asyncio.CancelledError, Exception):
                await task

        await self.client.close()

    def _record_observation_latency(self, started_at: float) -> None:
        completed_at = time.time()
        self._last_poll_started_at_ms = int(started_at * 1000)
        self._last_poll_completed_at_ms = int(completed_at * 1000)
        self._last_poll_latency_ms = round((completed_at - started_at) * 1000, 2)

    def _apply_head_update(self, head_cursor: int, *, source: str) -> bool:
        if self._head_cursor is not None and head_cursor <= self._head_cursor:
            return False

        self._head_cursor = head_cursor
        self._last_update_ts = time.time()
        self._last_head_source = source

        if self.logger:
            self.logger.debug(
                "block_tracker.update",
                component="tracker",
                head_cursor=head_cursor,
                source=source,
            )
        return True

    async def _run_poll(self):
        while self._running:
            try:
                started_at = time.time()
                request = build_eth_blockNumber()
                result = await self.client.execute(request, trace_request=False)
                self._record_observation_latency(started_at)

                if isinstance(result, str):
                    head_cursor = int(result, 16)
                    self._apply_head_update(head_cursor, source="poll")
                else:
                    if self.logger:
                        self.logger.warn(
                            "block_tracker.invalid_response",
                            component="tracker",
                            source="poll",
                            result=str(result)[:200],
                        )

                if self.logger:
                    self.logger.debug(
                        "block_tracker.latency",
                        component="tracker",
                        source="poll",
                        latency_ms=self._last_poll_latency_ms,
                    )

            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if self.logger:
                    self.logger.error(
                        "block_tracker.error",
                        component="tracker",
                        source="poll",
                        error=str(exc),
                    )

            await asyncio.sleep(self.poll_interval)

    async def _ensure_ws_session(self) -> aiohttp.ClientSession:
        if self._ws_session is None or self._ws_session.closed:
            self._ws_session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=30),
                headers={
                    "Accept": "application/json",
                    "Accept-Encoding": "gzip, deflate",
                },
            )
        return self._ws_session

    @staticmethod
    def _decode_json(data):
        if isinstance(data, (bytes, bytearray, memoryview)):
            return orjson.loads(bytes(data))
        return orjson.loads(str(data).encode("utf-8"))

    @staticmethod
    def _extract_ws_head_cursor(payload) -> int | None:
        if not isinstance(payload, dict):
            return None
        if payload.get("method") != "eth_subscription":
            return None
        params = payload.get("params") or {}
        result = params.get("result") or {}
        if not isinstance(result, dict):
            return None
        number = result.get("number")
        if not isinstance(number, str):
            return None
        try:
            return int(number, 16)
        except ValueError:
            return None

    async def _run_websocket(self):
        if not self.websocket_url:
            return

        reconnect_delay = max(float(self.websocket_reconnect_delay_sec), 0.5)
        while self._running:
            try:
                session = await self._ensure_ws_session()
                async with session.ws_connect(self.websocket_url, heartbeat=30) as ws:
                    if self.logger:
                        self.logger.info(
                            "block_tracker.ws_connected",
                            component="tracker",
                            websocket_url=self.websocket_url,
                        )

                    await ws.send_json(
                        {
                            "jsonrpc": "2.0",
                            "id": "1",
                            "method": "eth_subscribe",
                            "params": ["newHeads"],
                        }
                    )
                    subscription_id = None

                    while self._running:
                        msg = await ws.receive()
                        if not self._running:
                            break

                        if msg.type in {aiohttp.WSMsgType.TEXT, aiohttp.WSMsgType.BINARY}:
                            try:
                                payload = self._decode_json(msg.data)
                            except Exception as exc:
                                if self.logger:
                                    self.logger.warn(
                                        "block_tracker.ws_decode_error",
                                        component="tracker",
                                        websocket_url=self.websocket_url,
                                        error=str(exc),
                                    )
                                continue

                            if isinstance(payload, dict) and payload.get("id") == "1" and "result" in payload:
                                subscription_id = payload.get("result")
                                if self.logger:
                                    self.logger.info(
                                        "block_tracker.ws_subscribed",
                                        component="tracker",
                                        websocket_url=self.websocket_url,
                                        subscription_id=subscription_id,
                                    )
                                continue

                            head_cursor = self._extract_ws_head_cursor(payload)
                            if head_cursor is not None:
                                self._apply_head_update(head_cursor, source="websocket")
                                if self.logger:
                                    self.logger.debug(
                                        "block_tracker.ws_update",
                                        component="tracker",
                                        websocket_url=self.websocket_url,
                                        subscription_id=subscription_id,
                                        head_cursor=head_cursor,
                                    )
                                continue

                        if msg.type in {
                            aiohttp.WSMsgType.CLOSED,
                            aiohttp.WSMsgType.CLOSING,
                            aiohttp.WSMsgType.ERROR,
                        }:
                            raise ConnectionError(f"websocket closed with type={msg.type!s}")

            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if self.logger:
                    self.logger.error(
                        "block_tracker.ws_error",
                        component="tracker",
                        websocket_url=self.websocket_url,
                        error=str(exc),
                    )
                if not self._running:
                    break
                await asyncio.sleep(reconnect_delay)


ChainHeadTracker = EvmChainHeadTracker
