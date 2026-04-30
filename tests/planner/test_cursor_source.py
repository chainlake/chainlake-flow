import asyncio

from types import SimpleNamespace

from rpcstream.planner.cursor_source import (
    BackfillCursorSource,
    RealtimeCursorSource,
    build_cursor_source,
)


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


def test_backfill_cursor_source_emits_bounded_range():
    async def run():
        source = BackfillCursorSource(start=10, end=12)
        return [
            await source.next_cursor(),
            await source.next_cursor(),
            await source.next_cursor(),
            await source.next_cursor(),
        ]

    assert asyncio.run(run()) == [10, 11, 12, None]


def test_realtime_cursor_source_chainhead_starts_from_head():
    async def run():
        source = RealtimeCursorSource(
            DummyTracker([100, 100, 101]),
            start_cursor="chainhead",
        )
        return [
            await source.next_cursor(),
            await source.next_cursor(),
        ]

    assert asyncio.run(run()) == [100, 101]


def test_realtime_cursor_source_numeric_start_catches_up_then_tails():
    async def run():
        source = RealtimeCursorSource(
            DummyTracker([105, 105, 106, 107]),
            start_cursor=103,
        )
        return [
            await source.next_cursor(),
            await source.next_cursor(),
            await source.next_cursor(),
            await source.next_cursor(),
            await source.next_cursor(),
        ]

    assert asyncio.run(run()) == [103, 104, 105, 106, 107]


def test_build_cursor_source_resumes_backfill_after_checkpoint():
    runtime = SimpleNamespace(
        pipeline=SimpleNamespace(mode="backfill", start_cursor=10, end_cursor=15)
    )

    async def run():
        source = build_cursor_source(runtime, tracker=None, resume_cursor=12)
        return [await source.next_cursor(), await source.next_cursor()]

    assert asyncio.run(run()) == [13, 14]


def test_build_cursor_source_resumes_realtime_after_checkpoint():
    runtime = SimpleNamespace(
        pipeline=SimpleNamespace(mode="realtime", start_cursor="checkpoint", end_cursor=None)
    )

    async def run():
        source = build_cursor_source(runtime, DummyTracker([105, 106]), resume_cursor=103)
        return [await source.next_cursor(), await source.next_cursor()]

    assert asyncio.run(run()) == [104, 105]


def test_build_cursor_source_chainhead_ignores_saved_commit_watermark():
    runtime = SimpleNamespace(
        pipeline=SimpleNamespace(mode="realtime", start_cursor="chainhead", end_cursor=None)
    )

    async def run():
        source = build_cursor_source(runtime, DummyTracker([105, 106]), resume_cursor=103)
        return [await source.next_cursor(), await source.next_cursor()]

    assert asyncio.run(run()) == [105, 106]


def test_build_cursor_source_explicit_realtime_start_ignores_saved_commit_watermark():
    runtime = SimpleNamespace(
        pipeline=SimpleNamespace(mode="realtime", start_cursor=100, end_cursor=None)
    )

    async def run():
        source = build_cursor_source(runtime, DummyTracker([105, 106]), resume_cursor=103)
        return [
            await source.next_cursor(),
            await source.next_cursor(),
            await source.next_cursor(),
        ]

    assert asyncio.run(run()) == [100, 101, 102]
