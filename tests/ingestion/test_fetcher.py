import asyncio
import time

from rpcstream.adapters.evm.fetcher import EvmRpcFetcher


class DummyScheduler:
    async def submit_request(self, request):
        await asyncio.sleep(0.05)
        return request.method, object()


def test_evm_fetcher_submits_independent_rpc_requests_concurrently():
    fetcher = EvmRpcFetcher(
        DummyScheduler(),
        entities=["transaction", "receipt", "trace"],
    )

    async def run():
        started = time.perf_counter()
        result = await fetcher.fetch(100)
        return time.perf_counter() - started, result

    elapsed, result = asyncio.run(run())

    assert elapsed < 0.12
    assert set(result) == {"transaction", "receipt", "trace"}
