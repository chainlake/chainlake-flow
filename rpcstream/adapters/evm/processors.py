from __future__ import annotations

from rpcstream.adapters.evm.parser.parse_blocks import parse_blocks
from rpcstream.adapters.evm.parser.parse_receipts_logs import parse_receipts
from rpcstream.adapters.evm.parser.parse_transactions import parse_transactions
from rpcstream.adapters.evm.parser.parse_traces import parse_traces_auto


class BlockProcessor:
    def process(self, cursor, value):
        block = parse_blocks(value)
        return {"block": [block]}


class TransactionProcessor:
    def process(self, cursor, value):
        block = parse_blocks(value)
        txs = parse_transactions(value)
        return {"block": [block], "transaction": txs}


class ReceiptLogProcessor:
    def process(self, cursor, value):
        receipts, logs = parse_receipts(value)
        return {"receipt": receipts, "log": logs}


class TraceProcessor:
    def process(self, cursor, value):
        traces = parse_traces_auto(value, cursor, "debug_trace")
        return {"trace": traces}


PROCESSOR_REGISTRY = {
    "block": BlockProcessor(),
    "transaction": TransactionProcessor(),
    "receipt": ReceiptLogProcessor(),
    "log": ReceiptLogProcessor(),
    "trace": TraceProcessor(),
}
