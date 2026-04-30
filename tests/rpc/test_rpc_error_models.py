import importlib.util
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parents[2] / "rpcstream" / "client" / "models.py"
SPEC = importlib.util.spec_from_file_location("rpcstream_client_models_test", MODULE_PATH)
MODELS = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODELS)

RpcResponseError = MODELS.RpcResponseError
exception_log_fields = MODELS.exception_log_fields
is_expected_rpc_warning = MODELS.is_expected_rpc_warning
summarize_exception = MODELS.summarize_exception


def test_rpc_response_error_summarizes_upstream_block_not_ready():
    error = RpcResponseError.from_payload(
        method="eth_getBlockReceipts",
        request_meta={"cursor": 94349617},
        error={
            "code": -32603,
            "message": "upstream does not have the requested block yet",
            "data": {
                "code": "ErrUpstreamsExhausted",
                "details": {
                    "networkId": "evm:56",
                    "projectId": "main",
                    "upstreams": 31,
                    "durationMs": 2009,
                },
                "cause": [
                    {
                        "code": "ErrUpstreamBlockUnavailable",
                        "message": "upstream does not have the requested block yet",
                        "details": {
                            "blockNumber": 94349617,
                            "latestBlock": 94349610,
                            "finalizedBlock": 94349605,
                        },
                    },
                    {
                        "code": "ErrUpstreamBlockUnavailable",
                        "message": "upstream does not have the requested block yet",
                        "details": {
                            "blockNumber": 94349617,
                            "latestBlock": 94349616,
                            "finalizedBlock": 94349605,
                        },
                    },
                ],
            },
        },
    )

    assert error.is_upstream_block_not_ready() is True
    assert is_expected_rpc_warning(error) is True
    assert summarize_exception(error) == (
        "rpc_response_error(method=eth_getBlockReceipts, code=-32603, "
        "message=upstream does not have the requested block yet, cursor=94349617)"
    )

    fields = exception_log_fields(error)
    assert fields["cursor"] == 94349617
    assert fields["network_id"] == "evm:56"
    assert fields["project_id"] == "main"
    assert fields["upstreams_total"] == 31
    assert fields["not_ready_upstreams"] == 2
    assert fields["min_latest_cursor"] == 94349610
    assert fields["max_latest_cursor"] == 94349616


def test_rpc_response_error_non_expected_warning_keeps_basic_fields():
    error = RpcResponseError.from_payload(
        method="eth_blockNumber",
        error={
            "code": -32000,
            "message": "execution reverted",
        },
    )

    assert error.is_upstream_block_not_ready() is False
    assert is_expected_rpc_warning(error) is False
    assert exception_log_fields(error) == {
        "rpc_error_code": -32000,
        "rpc_error_message": "execution reverted",
    }
