# tests/rpc/test_rpc_client_sit.py
import asyncio
import pytest
from aiohttp import ClientConnectionError
from rpcstream.rpc.rpc_client import RpcClient

# -------------------------
# config
# -------------------------
RPC_URL = "http://localhost:30040/main/evm/56"  # erpc url
TIMEOUT = 5  # seconds


# -------------------------
# Test: RPC success against real ERPC
# -------------------------
@pytest.mark.asyncio
async def test_rpc_call_real_erpc():
    client = RpcClient(base_url=RPC_URL, max_retries=2, timeout_sec=TIMEOUT)

    result = await client.call("eth_blockNumber", [])
    
    block_number = int(result, 16)
    print("Block number (int):", block_number)

    assert isinstance(block_number, int)
    assert block_number > 0


# -------------------------
# Test: Transport error / max_retries
# -------------------------
@pytest.mark.asyncio
async def test_rpc_transport_error_sit():
    # simulate transport error
    client = RpcClient(base_url="http://localhost:9999", max_retries=2, timeout_sec=1)

    with pytest.raises(ClientConnectionError):
        await client.call("eth_getBlockByNumber", [])

    expected_attempts = client.max_retries + 1
    assert client.metrics.transport_error_total == expected_attempts