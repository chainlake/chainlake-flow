import asyncio

from types import SimpleNamespace

from rpcstream.planner.block_source import BackfillBlockSource, RealtimeBlockSource, build_block_source


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


def test_build_block_source_resumes_backfill_after_checkpoint():
    runtime = SimpleNamespace(
        pipeline=SimpleNamespace(mode="backfill", start_block=10, end_block=15)
    )

    async def run():
        source = build_block_source(runtime, tracker=None, resume_cursor=12)
        return [await source.next_block(), await source.next_block()]

    assert asyncio.run(run()) == [13, 14]


def test_build_block_source_resumes_realtime_after_checkpoint():
    runtime = SimpleNamespace(
        pipeline=SimpleNamespace(mode="realtime", start_block="latest", end_block=None)
    )

    async def run():
        source = build_block_source(runtime, DummyTracker([105, 106]), resume_cursor=103)
        return [await source.next_block(), await source.next_block()]

    assert asyncio.run(run()) == [104, 105]
