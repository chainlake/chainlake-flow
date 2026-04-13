from typing import List, Dict, Any, Iterator
from rpcstream.adapters.base import BaseRpcRequest


class EvmRpcRequest(BaseRpcRequest):
    """
    Production-ready RPC request object for EVM chains.
    Inherits from BaseRpcRequest to be compatible with a generic scheduler.
    """
    def transport_type(self) -> str:
      return "jsonrpc"


# --------------------------
# Builder functions
# --------------------------

def build_eth_blockNumber() -> EvmRpcRequest:
    """Create a request to get the latest block number"""
    return EvmRpcRequest(
        method="eth_blockNumber",
        params=[],
        request_id="latest",
        meta={"rpc": "blockNumber"},
    )


def build_get_block_by_number(block_number: int, include_transactions: bool = True) -> EvmRpcRequest:
    """Create a request to get a block by number, optionally including full transactions"""
    return EvmRpcRequest(
        method="eth_getBlockByNumber",
        params=[hex(block_number), include_transactions],
        request_id=block_number,
        meta={
            "block_number": block_number,
            "rpc": "block",
        },
    )


def build_get_block_receipts(block_number: int) -> EvmRpcRequest:
    """Create a request to get all transaction receipts for a block"""
    return EvmRpcRequest(
        method="eth_getBlockReceipts",
        params=[hex(block_number)],
        request_id=block_number,
        meta={
            "block_number": block_number,
            "rpc": "receipts",
        },
    )

# trace_block method is only available for Ethereum
def build_trace_block(block_number: int) -> EvmRpcRequest:
    """Create a request to trace a block using callTracer"""
    return EvmRpcRequest(
        method="trace_block",
        params=[hex(block_number)],
        request_id=block_number,
        meta={
            "block_number": block_number,
            "rpc": "trace",
        },
    )

def build_debug_trace_block(block_number: int) -> EvmRpcRequest:
    """Create a request to trace a block using callTracer"""
    return EvmRpcRequest(
        method="debug_traceBlockByNumber",
        params=[hex(block_number), {'tracer': 'callTracer'}],
        request_id=block_number,
        meta={
            "block_number": block_number,
            "rpc": "trace",
        },
    )

# --------------------------
# Batch generators
# --------------------------

def batch_get_blocks_by_number(start: int, end: int, include_transactions: bool = True) -> Iterator[EvmRpcRequest]:
    """Yield a batch of block requests"""
    for b in range(start, end + 1):
        yield build_get_block_by_number(b, include_transactions)


def batch_get_block_receipts(start: int, end: int) -> Iterator[EvmRpcRequest]:
    """Yield a batch of receipt requests"""
    for b in range(start, end + 1):
        yield build_get_block_receipts(b)


def batch_trace_blocks(start: int, end: int) -> Iterator[EvmRpcRequest]:
    """Yield a batch of trace requests"""
    for b in range(start, end + 1):
        yield build_trace_block(b)