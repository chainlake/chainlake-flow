from dataclasses import dataclass
from typing import Any


@dataclass
class RpcTaskMeta:
    task_id: int
    submit_ts: float
    extra: dict[str, Any]


@dataclass
class RangeResult:
    range_id: int
    start_block: int
    end_block: int
    payload: Any
    meta: RpcTaskMeta
