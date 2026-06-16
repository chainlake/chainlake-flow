import asyncio

from rpcstream.client.models import RpcResponseError
from rpcstream.protocol.request import BaseRpcRequest
from rpcstream.scheduler.adaptive import AdaptiveRpcScheduler


class DummyClient:
    async def execute(self, request):
        raise RpcResponseError.from_payload(
            method=request.method,
            request_meta={"cursor": 100},
            error={
                "code": -32603,
                "message": "upstream does not have the requested block yet",
                "data": {
                    "code": "ErrUpstreamsExhausted",
                    "details": {
                        "networkId": "evm:56",
                        "projectId": "main",
                        "upstreams": 19,
                        "durationMs": 1,
                    },
                    "cause": [
                        {
                            "code": "ErrUpstreamBlockUnavailable",
                            "message": "upstream does not have the requested block yet",
                            "details": {
                                "latestBlock": 100,
                                "finalizedBlock": 95,
                            },
                        }
                    ],
                },
            },
        )


def test_expected_upstream_warning_does_not_shrink_scheduler_window():
    scheduler = AdaptiveRpcScheduler(
        DummyClient(),
        min_inflight=1,
        max_inflight=8,
        initial_inflight=5,
        latency_target_ms=100,
    )
    request = BaseRpcRequest(method="eth_getBlockReceipts", meta={"cursor": 100})

    async def run():
        return await scheduler.submit_request(request)

    result = asyncio.run(run())

    assert result.expected is True
    assert scheduler.current_limit == 5
