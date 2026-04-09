# tests/rpc/test_rpc_client_ut.py

import asyncio
import pytest
from unittest.mock import patch, AsyncMock
from aiohttp import ClientConnectionError
from rpcstream.rpc.rpc_client import RpcClient


# -------------------------
# Helper: create a mock response compatible with 'async with'
# -------------------------
def make_mock_response(json_bytes: bytes = b'{"jsonrpc":"2.0","result":42,"id":"1"}') -> AsyncMock:
    """
    Return an AsyncMock that can be used in 'async with client.session.post(...)'.
    """
    mock_resp = AsyncMock()
    mock_resp.__aenter__.return_value = mock_resp
    mock_resp.__aexit__.return_value = None
    mock_resp.read = AsyncMock(return_value=json_bytes)
    mock_resp.raise_for_status = lambda: None
    return mock_resp


# -------------------------
# Helper: create a mock post that raises an exception in async with
# -------------------------
def make_mock_post_exception(exc: Exception):
    """
    Returns an AsyncMock that can be used with 'async with', but raises 'exc' in __aenter__.
    """
    mock_resp = AsyncMock()
    mock_resp.__aenter__.side_effect = exc
    mock_resp.__aexit__.return_value = None
    return AsyncMock(return_value=mock_resp)

# -------------------------
# Test: RPC success
# -------------------------
@pytest.mark.asyncio
async def test_rpc_success():
    client = RpcClient(base_url="http://fakeurl")

    mock_resp = make_mock_response()
    with patch.object(client.session, "post", return_value=mock_resp):
        result = await client.call("eth_blockNumber", [])
        assert result == 42


# -------------------------
# Test: RPC timeout retry
# -------------------------
@pytest.mark.asyncio
async def test_rpc_timeout_retry():
    client = RpcClient(base_url="http://fakeurl", max_retries=2)

    mock_success = make_mock_response(b'{"jsonrpc":"2.0","result":99,"id":"1"}')

    calls = [asyncio.TimeoutError(), asyncio.TimeoutError(), mock_success]

    def side_effect(*args, **kwargs):
        call = calls.pop(0)
        if isinstance(call, Exception):
            raise call
        return call

    with patch.object(client.session, "post", side_effect=side_effect):
        result = await client.call("eth_blockNumber", [])
        assert result == 99


# -------------------------
# Test: RPC error response
# -------------------------
@pytest.mark.asyncio
async def test_rpc_error_response():
    client = RpcClient(base_url="http://fakeurl")

    mock_resp = make_mock_response(b'{"jsonrpc":"2.0","error":"rpc failed","id":"1"}')
    with patch.object(client.session, "post", return_value=mock_resp):
        with pytest.raises(RuntimeError):
            await client.call("eth_blockNumber", [])


# -------------------------
# Test: Latency EMA update
# -------------------------
@pytest.mark.asyncio
async def test_latency_ema_update():
    client = RpcClient(base_url="http://fakeurl")

    mock_resp = make_mock_response()
    with patch.object(client.session, "post", return_value=mock_resp):
        prev_ema = client.metrics.latency_ema_ms
        await client.call("eth_blockNumber", [])
        assert client.metrics.latency_ema_ms != prev_ema
        

class MockPost:
    def __init__(self, exc):
        self.exc = exc

    async def __aenter__(self):
        raise self.exc

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return None

def mock_post(*args, **kwargs):
    return MockPost(ClientConnectionError("Connection failed"))

# -------------------------
# Test: RPC transport error
# -------------------------
@pytest.mark.asyncio
async def test_transport_error_metrics():
    client = RpcClient(base_url="http://fakeurl")

    with patch.object(client.session, "post", mock_post):
        import pytest

        with pytest.raises(ClientConnectionError):
            await client.call("eth_blockNumber", [])

        # transport_error_total expected to be max_retries + 1
        expected = client.max_retries + 1
        assert client.metrics.transport_error_total == expected


# -------------------------
# Test: Max retries exhausted
# -------------------------
@pytest.mark.asyncio
async def test_max_retries_exhausted():
    client = RpcClient(base_url="http://fakeurl", max_retries=2)

    with patch.object(client.session, "post", mock_post):
        import pytest

        with pytest.raises(ClientConnectionError) as exc_info:
            await client.call("eth_blockNumber", [])

        expected_attempts = client.max_retries + 1
        assert client.metrics.transport_error_total == expected_attempts
        assert client.metrics.retry_total == client.max_retries
        assert client.metrics.request_total == 1 

        assert "Connection failed" in str(exc_info.value)

        
