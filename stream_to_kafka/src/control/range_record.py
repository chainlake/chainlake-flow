from dataclasses import dataclass, field
import time
from .range_status import RangeStatus


# 控制面 / 状态机, 不存logs
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

    def touch(self):
        self.updated_ts = time.time()
