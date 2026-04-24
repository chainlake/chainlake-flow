import asyncio

from rpcstream.planner.block_source import BackfillBlockSource, RealtimeBlockSource


class DummyTracker:
    def __init__(self, values):
        self.values = list(values)
        self._index = 0

    def get_latest(self):
        if self._index >= len(self.values):
            return self.values[-1]
        value = self.values[self._index]
        self._index += 1
        return value


def test_backfill_block_source_emits_bounded_range():
    async def run():
        source = BackfillBlockSource(start=10, end=12)
        return [
            await source.next_block(),
            await source.next_block(),
            await source.next_block(),
            await source.next_block(),
        ]

    assert asyncio.run(run()) == [10, 11, 12, None]


def test_realtime_block_source_latest_starts_from_head():
    async def run():
        source = RealtimeBlockSource(DummyTracker([100, 100, 101]), start_block="latest")
        return [
            await source.next_block(),
            await source.next_block(),
        ]

    assert asyncio.run(run()) == [100, 101]


def test_realtime_block_source_numeric_start_catches_up_then_tails():
    async def run():
        source = RealtimeBlockSource(DummyTracker([105, 105, 106, 107]), start_block=103)
        return [
            await source.next_block(),
            await source.next_block(),
            await source.next_block(),
            await source.next_block(),
            await source.next_block(),
        ]

    assert asyncio.run(run()) == [103, 104, 105, 106, 107]
