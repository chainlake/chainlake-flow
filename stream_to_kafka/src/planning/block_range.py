from dataclasses import dataclass

@dataclass
class BlockRange:
    range_id: int
    start_block: int
    end_block: int
