from rpcstream.adapters.evm.enrich import EvmEnricher


def test_evm_enricher_merges_receipt_fields_into_transactions():
    bundle = {
        "transaction": [
            {
                "type": "transaction",
                "hash": "0xtx",
                "block_number": 1,
            }
        ],
        "receipt": [
            {
                "type": "receipt",
                "transaction_hash": "0xtx",
                "status": 1,
                "gas_used": 21000,
            }
        ],
    }

    enriched = EvmEnricher().enrich(bundle)

    assert enriched["transaction"][0]["receipt_status"] == 1
    assert enriched["transaction"][0]["receipt_gas_used"] == 21000
    assert enriched["transaction"][0]["receipt_contract_address"] is None
    assert "receipt_transaction_hash" not in enriched["transaction"][0]
    assert "receipt_transaction_index" not in enriched["transaction"][0]
    assert "receipt_block_hash" not in enriched["transaction"][0]
    assert "receipt_block_number" not in enriched["transaction"][0]
    assert "receipt_from_address" not in enriched["transaction"][0]
    assert "receipt_to_address" not in enriched["transaction"][0]


def test_evm_enricher_injects_block_context_into_logs_and_traces():
    bundle = {
        "block": [
            {
                "type": "block",
                "number": 11,
                "hash": "0xblock",
                "timestamp": 12345,
            }
        ],
        "log": [
            {
                "type": "log",
                "block_number": 11,
            }
        ],
        "token_transfer": [
            {
                "type": "token_transfer",
                "block_number": 11,
            }
        ],
        "trace": [
            {
                "type": "trace",
                "block_number": 11,
            }
        ],
    }

    enriched = EvmEnricher().enrich(bundle)

    assert enriched["log"][0]["block_hash"] == "0xblock"
    assert enriched["log"][0]["block_timestamp"] == 12345
    assert enriched["token_transfer"][0]["block_hash"] == "0xblock"
    assert enriched["token_transfer"][0]["block_timestamp"] == 12345
    assert enriched["trace"][0]["block_hash"] == "0xblock"
    assert enriched["trace"][0]["block_timestamp"] == 12345
