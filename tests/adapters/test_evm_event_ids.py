from rpcstream.adapters.evm.identity.event_id_calculator import EventIdCalculator


def test_token_transfer_uses_log_style_id():
    calculator = EventIdCalculator()
    item = {
        "type": "token_transfer",
        "transaction_hash": "0x392df770fd46ed80400cf1adf2d9aab4b1e0e2817728ffbbf2af5343fbd1c521",
        "log_index": 118,
        "transfer_index": 0,
    }

    assert (
        calculator.calculate_event_id(item)
        == "token_transfer_0x392df770fd46ed80400cf1adf2d9aab4b1e0e2817728ffbbf2af5343fbd1c521_118_0"
    )
