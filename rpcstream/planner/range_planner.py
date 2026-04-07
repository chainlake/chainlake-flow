from abc import ABC, abstractmethod
from typing import Optional

from blockchain_ingestion.planner.block_window import BlockRange


class BaseRangePlanner(ABC):
    @abstractmethod
    def next_range(self, latest_block: int) -> Optional[BlockRange]:
        raise NotImplementedError

    @property
    def exhausted(self) -> bool:
        return False


class TailingRangePlanner(BaseRangePlanner):
    def __init__(self, start_block: int, range_size: int):
        self._next_block = start_block
        self._range_size = range_size
        self._next_range_id = 0

    def next_range(self, latest_block: int) -> Optional[BlockRange]:
        if self._next_block > latest_block:
            return None

        start = self._next_block
        end = min(start + self._range_size - 1, latest_block)
        block_range = BlockRange(self._next_range_id, start, end)

        self._next_block = end + 1
        self._next_range_id += 1
        return block_range


class BoundedRangePlanner(BaseRangePlanner):
    def __init__(self, start_block: int, end_block: int, range_size: int):
        if start_block > end_block:
            raise ValueError("start_block must be <= end_block")

        self._next_block = start_block
        self._end_block = end_block
        self._range_size = range_size
        self._next_range_id = 0

    def next_range(self, latest_block: int) -> Optional[BlockRange]:
        if self._next_block > self._end_block:
            return None

        upper = min(latest_block, self._end_block)
        if self._next_block > upper:
            return None

        start = self._next_block
        end = min(start + self._range_size - 1, upper)
        block_range = BlockRange(self._next_range_id, start, end)

        self._next_block = end + 1
        self._next_range_id += 1
        return block_range

    @property
    def exhausted(self) -> bool:
        return self._next_block > self._end_block
