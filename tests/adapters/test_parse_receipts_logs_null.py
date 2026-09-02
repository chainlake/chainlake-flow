from rpcstream.adapters.evm.parser import parse_receipts


def test_parse_receipts_tolerates_null_logs():
    """Some upstream receipts carry "logs": null (legacy/early blocks)
    instead of an empty array. parse_receipts must treat that as no logs,
    not raise 'NoneType' object is not iterable -- a raised cursor would
    otherwise create an unresolved watermark gap and stall progress."""
    receipt = {
        "blockNumber": "0x1",
        "blockHash": "0xaa",
        "transactionHash": "0xbb",
        "transactionIndex": "0x0",
        "logs": None,
    }
    receipt_rows, log_rows = parse_receipts([receipt])
    assert len(receipt_rows) == 1
    assert log_rows == []


def test_parse_receipts_missing_logs_key():
    """A receipt with the logs key entirely absent still parses."""
    receipt = {
        "blockNumber": "0x2",
        "blockHash": "0xcc",
        "transactionHash": "0xdd",
        "transactionIndex": "0x1",
    }
    receipt_rows, log_rows = parse_receipts([receipt])
    assert len(receipt_rows) == 1
    assert log_rows == []


def test_parse_receipts_null_block_receipts():
    """eth_getBlockReceipts can return null (not []) for blocks without
    transactions; parse_receipts must yield an empty result, not crash."""
    receipt_rows, log_rows = parse_receipts(None)
    assert receipt_rows == []
    assert log_rows == []
