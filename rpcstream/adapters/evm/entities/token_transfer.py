from __future__ import annotations

from dataclasses import dataclass


@dataclass
class TokenTransfer:
    type: str = "token_transfer"
    transfer_type: str | None = None
    source_log_id: str | None = None
    id: str | None = None
    token_address: str | None = None
    from_address: str | None = None
    to_address: str | None = None
    token_id: str | None = None
    amount: str | None = None
    transaction_hash: str | None = None
    transaction_index: int | None = None
    block_hash: str | None = None
    block_number: int | None = None
    block_timestamp: int | None = None
    log_index: int | None = None
    transfer_index: int | None = None
