from __future__ import annotations

import asyncio
import time

from rpcstream.adapters.evm.rpc_requests import build_eth_blockNumber


class EvmChainHeadTracker:
    def __init__(
        self,
        client,
        poll_interval=0.2,
        logger=None,
    ):
        self.client = client
        self.poll_interval = poll_interval
        self.logger = logger

        self._head_cursor = None
        self._running = False
        self._task = None

        self._last_update_ts = 0

    def get_head_cursor(self):
        return self._head_cursor

    def get_latest(self):
        return self.get_head_cursor()

    def get_lag(self, current_cursor):
        if self._head_cursor is None:
            return None
        return self._head_cursor - current_cursor

    async def start(self):
        self._running = True
        self._task = asyncio.create_task(self._run())

    async def stop(self):
        self._running = False
        if self._task:
            await self._task
        await self.client.close()

    async def _run(self):
        while self._running:
            try:
                start = time.time()

                request = build_eth_blockNumber()
                result = await self.client.execute(request, trace_request=False)

                latency = (time.time() - start) * 1000

                self.logger.debug("block_tracker.latency", latency_ms=latency)

                if isinstance(result, str):
                    head_cursor = int(result, 16)

                    if head_cursor != self._head_cursor:
                        self._head_cursor = head_cursor
                        self._last_update_ts = time.time()

                        if self.logger:
                            self.logger.debug(
                                "block_tracker.update",
                                component="tracker",
                                head_cursor=head_cursor,
                            )
                else:
                    if self.logger:
                        self.logger.warn(
                            "block_tracker.invalid_response",
                            component="tracker",
                            result=str(result)[:200],
                        )

            except Exception as e:
                if self.logger:
                    self.logger.error(
                        "block_tracker.error",
                        component="tracker",
                        error=str(e),
                    )

            await asyncio.sleep(self.poll_interval)

ChainHeadTracker = EvmChainHeadTracker
