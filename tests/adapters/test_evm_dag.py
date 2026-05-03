from rpcstream.adapters.evm.dag import resolve_internal_entities
from rpcstream.adapters.evm.dag import resolve_sink_entities


def test_token_transfer_is_sink_only():
    assert resolve_sink_entities(["token_transfer"]) == ["token_transfer"]
    assert resolve_internal_entities(["token_transfer"]) == ["block", "receipt", "log"]


def test_transaction_internal_entities_include_block():
    assert resolve_internal_entities(["transaction"]) == ["block", "transaction", "receipt"]
