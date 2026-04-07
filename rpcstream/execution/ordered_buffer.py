from blockchain_ingestion.execution.result_merger import RangeResult


class OrderedResultBuffer:
    def __init__(self):
        self._buffer: dict[int, RangeResult] = {}
        self._next_range_id = 0

    def add(self, result: RangeResult) -> None:
        self._buffer[result.range_id] = result

    def pop_ready(self) -> list[RangeResult]:
        ready: list[RangeResult] = []
        while self._next_range_id in self._buffer:
            ready.append(self._buffer.pop(self._next_range_id))
            self._next_range_id += 1
        return ready
