from typing import Optional
from .block_range import BlockRange
from abc import ABC, abstractmethod

# -------------------------
# 顺序生成
# 不感知执行结果
# 不 retry
# -------------------------
class BaseRangePlanner(ABC):
    @abstractmethod
    def next_range(self, latest_block: int) -> Optional[BlockRange]:
        pass

    @property
    def exhausted(self) -> bool:
        return False

# Resume = 有边界、有尽头的 backfill job
# Realtime = 无状态、永远追 head 的 daemon job

class TailingRangePlanner(BaseRangePlanner):
    """
    Realtime / streaming planner
    - 永远追最新块
    - 不设上界
    """
    def __init__(self, start_block: int, range_size: int):
        self._next_block = start_block
        self._range_size = range_size
        self._next_range_id = 0

    def next_range(self, latest_block: int) -> Optional[BlockRange]:
        if self._next_block > latest_block:
            return None

        start = self._next_block
        end = min(
            start + self._range_size - 1,
            latest_block,
        )

        r = BlockRange(
            range_id=self._next_range_id,
            start_block=start,
            end_block=end,
        )

        self._next_block = end + 1
        self._next_range_id += 1

        return r


class BoundedRangePlanner(BaseRangePlanner):
    """
    Resume / Backfill planner
    - 有明确 end_block
    - 不追新块
    - 生成完即结束
    """
    def __init__(
        self,
        start_block: int,
        end_block: int,
        range_size: int,
    ):
        if start_block > end_block:
            raise ValueError(
                f"start_block {start_block} > end_block {end_block}"
            )

        self._next_block = start_block
        self._end_block = end_block
        self._range_size = range_size
        self._next_range_id = 0

    def next_range(self, latest_block: int) -> Optional[BlockRange]:
        # hard stop
        if self._next_block > self._end_block:
            return None

        # 安全上限：不能超过 planner 的 end_block
        upper = min(latest_block, self._end_block)
        if self._next_block > upper:
            return None

        start = self._next_block
        end = min(
            start + self._range_size - 1,
            upper,
        )

        r = BlockRange(
            range_id=self._next_range_id,
            start_block=start,
            end_block=end,
        )

        self._next_block = end + 1
        self._next_range_id += 1

        return r

    @property
    def exhausted(self) -> bool:
        return self._next_block > self._end_block