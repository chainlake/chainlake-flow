from enum import Enum

# 控制面状态机，不属于数据、不属于执行。
class RangeStatus(str, Enum):
    PLANNED = "PLANNED"
    INFLIGHT = "INFLIGHT"
    RETRYING = "RETRYING"
    DONE = "DONE"
    FAILED = "FAILED"
