from dataclasses import dataclass


@dataclass(frozen=True)
class BlockRange:
    range_id: int
    start_block: int
    end_block: int
