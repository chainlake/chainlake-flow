from rpcstream.adapters.evm.parser.parse_blocks import parse_blocks
from rpcstream.adapters.evm.parser.parse_transactions import parse_transactions
from rpcstream.adapters.evm.parser.parse_receipts_logs import parse_receipts
from rpcstream.adapters.evm.parser.parse_traces import parse_traces_auto

class BlockProcessor:
    def process(self, block_number, value):
        # Process the block data
        block = parse_blocks(value)
        return {"block": [block]}

class TransactionProcessor:
    def process(self, block_number, value):
        # Process the transaction data
        block = parse_blocks(value)
        txs = parse_transactions(value)
        return {"block": [block], "transaction": txs}

class ReceiptLogProcessor:
    def process(self, block_number, value):
        # Process the receipt and logs
        receipts, logs = parse_receipts(value)
        return {"receipt": receipts, "log": logs}

class TraceProcessor:
    def process(self, block_number, value):
        # debug_traceBlockByNumber returns the trace payload directly as the RPC result.
        traces = parse_traces_auto(value, block_number, "debug_trace")
        return {"trace": traces}

# Processor Registry
PROCESSOR_REGISTRY = {
    "block": BlockProcessor(),
    "transaction": TransactionProcessor(),
    "receipt": ReceiptLogProcessor(),
    "log": ReceiptLogProcessor(),
    "trace": TraceProcessor(),
}
