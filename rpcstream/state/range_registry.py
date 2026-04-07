from dataclasses import dataclass, field
from enum import Enum
import time


class RangeStatus(str, Enum):
    PLANNED = "PLANNED"
    INFLIGHT = "INFLIGHT"
    RETRYING = "RETRYING"
    DONE = "DONE"
    FAILED = "FAILED"


@dataclass
class RangeRecord:
    range_id: int
    start_block: int
    end_block: int
    status: RangeStatus = RangeStatus.PLANNED
    retry: int = 0
    max_retry: int = 3
    last_error: str | None = None
    last_task_id: int | None = None
    created_ts: float = field(default_factory=time.time)
    updated_ts: float = field(default_factory=time.time)

    def touch(self) -> None:
        self.updated_ts = time.time()


class RangeRegistry:
    def __init__(self):
        self._ranges: dict[int, RangeRecord] = {}

    def register(self, range_id: int, start_block: int, end_block: int) -> RangeRecord:
        if range_id in self._ranges:
            raise RuntimeError(f"range {range_id} already registered")
        record = RangeRecord(range_id=range_id, start_block=start_block, end_block=end_block)
        self._ranges[range_id] = record
        return record

    def get(self, range_id: int) -> RangeRecord:
        return self._ranges[range_id]

    def mark_inflight(self, range_id: int, task_id: int) -> None:
        item = self.get(range_id)
        item.status = RangeStatus.INFLIGHT
        item.last_task_id = task_id
        item.touch()

    def mark_done(self, range_id: int) -> None:
        item = self.get(range_id)
        item.status = RangeStatus.DONE
        item.touch()
        self._ranges.pop(range_id, None)

    def mark_failed(self, range_id: int, error: str) -> None:
        item = self.get(range_id)
        item.status = RangeStatus.FAILED
        item.last_error = error
        item.touch()
        self._ranges.pop(range_id, None)

    def mark_retry(self, range_id: int, error: str) -> bool:
        item = self.get(range_id)
        item.retry += 1
        item.last_error = error
        item.touch()
        if item.retry > item.max_retry:
            item.status = RangeStatus.FAILED
            return False
        item.status = RangeStatus.RETRYING
        return True
