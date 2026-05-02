from rpcstream.adapters.evm.dag import resolve_internal_entities, resolve_sink_entities


def test_token_transfer_is_sink_only():
    assert resolve_sink_entities(["token_transfer"]) == ["token_transfer"]
    assert resolve_internal_entities(["token_transfer"]) == ["block", "receipt", "log"]
