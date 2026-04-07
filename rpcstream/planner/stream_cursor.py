import asyncio

from blockchain_ingestion.rpc.erpc_client import ErpcClient


class LatestBlockTracker:
    def __init__(self, client: ErpcClient, refresh_interval: float = 2.0):
        self.client = client
        self.refresh_interval = refresh_interval
        self._latest: int | None = None
        self._task: asyncio.Task | None = None
        self._stop = False

    def get_cached(self) -> int | None:
        return self._latest

    async def _run(self) -> None:
        while not self._stop:
            result = await self.client.call("eth_blockNumber", [])
            if isinstance(result, str):
                self._latest = int(result, 16)
            else:
                self._latest = int(result)
            await asyncio.sleep(self.refresh_interval)

    def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        self._stop = True
        if self._task:
            await self._task
