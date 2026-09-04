from __future__ import annotations

import json

from rpcstream.adapters.evm.parser.parse_receipts_logs import parse_receipts
from rpcstream.adapters.evm.parser.parse_transactions import parse_transactions
from rpcstream.adapters.evm.parser.parse_traces import parse_traces_auto


class BlockProcessor:
    def process(self, cursor, value):
        from rpcstream.adapters.evm.parser.parse_blocks import parse_blocks

        block = parse_blocks(value)
        return {"block": [block]}


class TransactionProcessor:
    def process(self, cursor, value):
        txs = parse_transactions(value)
        return {"transaction": txs}


class ReceiptLogProcessor:
    def process(self, cursor, value):
        receipts, logs = parse_receipts(value)
        return {"receipt": receipts, "log": logs}


class TraceProcessor:
    def process(self, cursor, value):
        traces = parse_traces_auto(value, cursor, "debug_trace")
        return {"trace": traces}


class RawEnvelopeProcessor:
    """Canonical-fetch: bundles raw block+receipts JSON into a single Kafka row.

    The output row carries block_timestamp so the engine can track ingestion lag,
    and kafka_partition_key=block_number so the envelope topic is partitioned
    deterministically (derived consumers can assign partitions by cursor range).
    """

    def process(self, cursor, value):
        block_json, receipts_json = value
        block_timestamp = None
        if isinstance(block_json, dict):
            ts = block_json.get("timestamp")
            if ts is not None:
                try:
                    block_timestamp = (
                        int(ts, 16) if isinstance(ts, str) and ts.startswith("0x") else int(ts)
                    )
                except (ValueError, TypeError):
                    pass
        row = {
            "kafka_partition_key": str(cursor),
            "block_number": cursor,
            "block_timestamp": block_timestamp,
            "block_json": json.dumps(block_json, separators=(",", ":")),
            "receipts_json": json.dumps(receipts_json, separators=(",", ":")),
        }
        return {"raw_envelope": [row]}


PROCESSOR_REGISTRY = {
    "block": BlockProcessor(),
    "transaction": TransactionProcessor(),
    "receipt": ReceiptLogProcessor(),
    "log": ReceiptLogProcessor(),
    "trace": TraceProcessor(),
    "raw_envelope": RawEnvelopeProcessor(),
}
