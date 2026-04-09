from typing import List, Any, Dict
from rpcstream.adapters.base import BaseRpcRequest

# --------------------------
# Sui-specific RPC request
# --------------------------
class SuiRpcRequest(BaseRpcRequest):
    """
    Production-ready RPC request object for SUI chains
    Inherits from BaseRpcRequest
    """
    pass  # All logic is handled by BaseRpcRequest; additional SUI-specific methods can be added if needed


# --------------------------
# Builder functions
# --------------------------

def build_get_latest_checkpoint() -> SuiRpcRequest:
    """Get the sequence number of the latest checkpoint that has been executed"""
    return SuiRpcRequest(
        method="sui_getLatestCheckpointSequenceNumber",
        params=[],
        request_id="",
        meta={"rpc": "checkpoint_number"},
    )

def build_get_total_transactions() -> SuiRpcRequest:
    """Get total number of transactions in SUI"""
    return SuiRpcRequest(
        method="sui_getTotalTransactionNumber",
        params=[],
        request_id="total_tx",
        meta={"rpc": "total_tx"},
    )


def build_get_checkpoint(checkpoint_id: int) -> SuiRpcRequest:
    """Get SUI checkpoint info by id"""
    return SuiRpcRequest(
        method="sui_getCheckpoint",
        params=[checkpoint_id],
        request_id=checkpoint_id,
        meta={"checkpoint_id": checkpoint_id, "rpc": "checkpoint"},
    )


# --------------------------
# Batch generators
# --------------------------

def batch_get_checkpoints(start: int, end: int) -> List[SuiRpcRequest]:
    """Yield a batch of checkpoint requests"""
    return [build_get_checkpoint(i) for i in range(start, end + 1)]