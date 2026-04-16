import asyncio
import time
from rpcstream.adapters.evm.rpc_requests import build_eth_blockNumber

# control-plane: Tracker → direct RPC (fast lane)
class BlockHeadTracker:
    def __init__(
        self,
        client,
        poll_interval=0.2,   # 200ms (BSC ~400ms block time)
        logger=None,
    ):
        self.client = client
        self.poll_interval = poll_interval
        self.logger = logger

        self._latest_block = None
        self._running = False
        self._task = None

        self._last_update_ts = 0

    # ----------------------------
    # Public API
    # ----------------------------
    def get_latest(self):
        return self._latest_block

    def get_lag(self, current_block):
        if self._latest_block is None:
            return None
        return self._latest_block - current_block

    # ----------------------------
    # Lifecycle
    # ----------------------------
    async def start(self):
        self._running = True
        self._task = asyncio.create_task(self._run())

    async def stop(self):
        self._running = False
        if self._task:
            await self._task
        await self.client.close()

    # ----------------------------
    # Core loop
    # ----------------------------
    async def _run(self):
        while self._running:
            try:
                start = time.time()
                
                request = build_eth_blockNumber()
                result = await self.client.execute(request)
                
                latency = (time.time() - start) * 1000
                
                self.logger.debug(
                    "block_tracker.latency",
                    latency_ms=latency
                )
                
                if isinstance(result, str):
                    latest = int(result, 16)

                    if latest != self._latest_block:
                        self._latest_block = latest
                        self._last_update_ts = time.time()

                        if self.logger:
                            self.logger.debug(
                                "block_tracker.update",
                                component="tracker",
                                latest_block=latest,
                            )

                else:
                    # unexpected response
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