from __future__ import annotations

from rpcstream.adapters.base import ChainAdapter
from rpcstream.adapters.evm.adapter import EvmChainAdapter
from rpcstream.adapters.sui.adapter import SuiChainAdapter


def build_chain_adapter(chain_type: str) -> ChainAdapter:
    normalized = str(chain_type).strip().lower()
    if normalized == "evm":
        return EvmChainAdapter()
    if normalized == "sui":
        return SuiChainAdapter()
    raise NotImplementedError(f"Unsupported chain adapter: {chain_type!r}")
