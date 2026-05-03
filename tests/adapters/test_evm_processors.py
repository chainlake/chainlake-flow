from rpcstream.adapters.evm.processors import BlockProcessor
from rpcstream.adapters.evm.processors import TransactionProcessor


def test_transaction_processor_does_not_emit_block_rows():
    block = {
        "type": "block",
        "number": "0x1",
        "hash": "0xblock",
        "timestamp": "0x2",
        "transactions": [
            {
                "hash": "0xtx1",
                "from": "0xfrom",
                "to": "0xto",
                "nonce": "0x0",
                "value": "0x0",
                "gas": "0x1",
                "gasPrice": "0x2",
                "maxFeePerGas": None,
                "maxPriorityFeePerGas": None,
                "maxFeePerBlobGas": None,
                "type": "0x0",
                "chainId": "0x38",
                "v": "0x1",
                "r": "0x1",
                "s": "0x2",
                "input": "0x",
                "blobVersionedHashes": [],
            }
        ],
    }

    processed = TransactionProcessor().process(96098686, block)

    assert "block" not in processed
    assert len(processed["transaction"]) == 1
    assert processed["transaction"][0]["hash"] == "0xtx1"
    assert processed["transaction"][0]["block_hash"] == "0xblock"
    assert processed["transaction"][0]["block_number"] == 1
    assert processed["transaction"][0]["block_timestamp"] == 2


def test_block_processor_emits_block_rows():
    block = {
        "type": "block",
        "number": "0x1",
        "hash": "0xblock",
        "parentHash": "0xparent",
        "timestamp": "0x2",
        "transactions": [],
    }

    processed = BlockProcessor().process(96098686, block)

    assert list(processed) == ["block"]
    assert len(processed["block"]) == 1
    assert processed["block"][0]["hash"] == "0xblock"
