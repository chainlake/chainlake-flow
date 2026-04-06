from .range_record import RangeRecord
from .range_status import RangeStatus

# from src.metrics.runtime import get_metrics

# _m = None
# def m():
#     global _m
#     if _m is None:
#         _m = get_metrics()
#     return _m

# -------------------------
# single source of truth”
# 生命周期管理器
# 不关心 RPC / Kafka
# -------------------------
class RangeRegistryNoMetrics:
    """
    Single source of truth for range lifecycle.
    """

    def __init__(self):
        self._ranges: dict[int, RangeRecord] = {}

    # -------------------------
    # register
    # -------------------------
    def register(self, range_id: int, start_block: int, end_block: int) -> RangeRecord:
        if range_id in self._ranges:
            raise RuntimeError(f"range {range_id} already registered")

        r = RangeRecord(
            range_id=range_id,
            start_block=start_block,
            end_block=end_block,
        )
        self._ranges[range_id] = r
        return r

    # -------------------------
    # lookup
    # -------------------------
    def get(self, range_id: int) -> RangeRecord:
        try:
            return self._ranges[range_id]
        except KeyError:
            raise KeyError(f"range {range_id} not found")

    # -------------------------
    # state transitions
    # -------------------------
    def mark_inflight(self, range_id: int, task_id: int):
        r = self.get(range_id)
        r.status = RangeStatus.INFLIGHT
        r.last_task_id = task_id
        r.touch()
        self._refresh_metrics()

    def mark_done(self, range_id: int):
        r = self.get(range_id)
        r.status = RangeStatus.DONE
        r.touch()
        self._ranges.pop(range_id, None)
        self._refresh_metrics()

    def mark_failed(self, range_id: int, error: str):
        r = self.get(range_id)
        r.status = RangeStatus.FAILED
        r.last_error = error
        r.touch()
        self._ranges.pop(range_id, None)
        self._refresh_metrics()
    # -------------------------
    # retry logic
    # -------------------------
    def mark_retry(self, range_id: int, error: str) -> bool:
        """
        Returns True if retry allowed, False if exhausted.
        """
        r = self.get(range_id)

        r.retry += 1
        r.last_error = error
        r.touch()

        if r.retry > r.max_retry:
            r.status = RangeStatus.FAILED
            return False

        r.status = RangeStatus.RETRYING
        self._refresh_metrics()
        return True

    # -------------------------
    # helpers
    # -------------------------
    def inflight_count(self) -> int:
        return sum(1 for r in self._ranges.values() if r.status == RangeStatus.INFLIGHT)

    def active_ranges(self) -> list[RangeRecord]:
        return [
            r for r in self._ranges.values()
            if r.status not in (RangeStatus.DONE, RangeStatus.FAILED)
        ]
        
    # 增加一个 统一刷新指标的方法
    def _refresh_metrics(self):
        inflight = 0
        retrying = 0
        active = 0

        for r in self._ranges.values():
            if r.status == RangeStatus.INFLIGHT:
                inflight += 1
            if r.status == RangeStatus.RETRYING:
                retrying += 1
            if r.status not in (RangeStatus.DONE, RangeStatus.FAILED):
                active += 1

        # m().range_inflight_set(inflight)