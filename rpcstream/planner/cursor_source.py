from __future__ import annotations

import asyncio
import time
from abc import ABC, abstractmethod

from rpcstream.runtime.observability.context import ObservabilityContext


class CursorSource(ABC):
    @abstractmethod
    async def next_cursor(self):
        """
        Return the next cursor to process.
        Return None when finished for bounded sources.
        """
        raise NotImplementedError

    async def next_block(self):
        """
        Compatibility shim for existing block-based callers.
        """
        return await self.next_cursor()


class BackfillCursorSource(CursorSource):
    """
    Deterministic bounded cursor replay:
    start → end (inclusive)
    """

    def __init__(
        self,
        start: int,
        end: int,
        delay_ms: float = 0,
        observability: ObservabilityContext | None = None,
    ):
        self.current = start
        self.end = end
        self.delay = delay_ms / 1000.0
        self._span = None
        self.observability = observability or ObservabilityContext.disabled()
        self._tracer = self.observability.get_tracer(__name__)

    async def next_cursor(self):
        if self._span is None:
            self._span = self._tracer.start_span("cursor_source.backfill_range")
            self._span.set_attribute("start", self.current)
            self._span.set_attribute("end", self.end)

        if self.current > self.end:
            if self._span:
                self._span.set_attribute("total_cursors", self.end - self.current + 1)
                self._span.end()
            return None

        cursor = self.current
        self.current += 1

        if self._span:
            self._span.add_event("cursor_emitted", {"cursor": cursor})

        if self.delay > 0:
            await asyncio.sleep(self.delay)

        return cursor


class RealtimeCursorSource(CursorSource):
    def __init__(
        self,
        tracker,
        start_cursor: int | str = "chainhead",
        observability: ObservabilityContext | None = None,
    ):
        self.tracker = tracker
        self.last_emitted_cursor = None
        self.start_cursor = start_cursor
        self.last_head_observed_at_ms = None
        self.last_head_source = None
        self.last_cursor_emitted_at_ms = None
        self.last_head_cursor = None
        self.observability = observability or ObservabilityContext.disabled()
        self._tracer = self.observability.get_tracer(__name__)

    async def next_cursor(self):
        while True:
            head_cursor = self.tracker.get_latest()

            if head_cursor is None:
                with self._tracer.start_as_current_span("cursor_source.wait_for_head") as span:
                    span.set_attribute("source", "realtime")
                    await asyncio.sleep(0.1)
                continue

            if self.last_emitted_cursor is None:
                if self.start_cursor in {"latest", "chainhead"}:
                    self.last_head_cursor = head_cursor
                    self.last_head_observed_at_ms = getattr(self.tracker, "get_last_update_at_ms", lambda: None)()
                    self.last_head_source = getattr(self.tracker, "get_last_head_source", lambda: None)()
                    self.last_cursor_emitted_at_ms = int(time.time() * 1000)
                    self.last_emitted_cursor = head_cursor
                    return head_cursor

                start_cursor = int(self.start_cursor)
                if start_cursor > head_cursor:
                    await asyncio.sleep(0.05)
                    continue

                self.last_head_cursor = head_cursor
                self.last_head_observed_at_ms = getattr(self.tracker, "get_last_update_at_ms", lambda: None)()
                self.last_head_source = getattr(self.tracker, "get_last_head_source", lambda: None)()
                self.last_cursor_emitted_at_ms = int(time.time() * 1000)
                self.last_emitted_cursor = start_cursor
                return start_cursor

            if head_cursor > self.last_emitted_cursor:
                self.last_head_cursor = head_cursor
                self.last_head_observed_at_ms = getattr(self.tracker, "get_last_update_at_ms", lambda: None)()
                self.last_head_source = getattr(self.tracker, "get_last_head_source", lambda: None)()
                self.last_cursor_emitted_at_ms = int(time.time() * 1000)
                self.last_emitted_cursor += 1
                return self.last_emitted_cursor

            await asyncio.sleep(0.05)


def build_cursor_source(
    runtime,
    tracker,
    observability: ObservabilityContext | None = None,
    resume_cursor: int | None = None,
) -> CursorSource:
    if runtime.pipeline.mode == "backfill":
        start = int(runtime.pipeline.start_cursor)
        if resume_cursor is not None:
            start = max(start, resume_cursor + 1)
        return BackfillCursorSource(
            start=start,
            end=int(runtime.pipeline.end_cursor),
            observability=observability,
        )

    start_cursor = runtime.pipeline.start_cursor
    if start_cursor == "checkpoint" and resume_cursor is not None:
        start_cursor = resume_cursor + 1
    elif start_cursor == "checkpoint":
        start_cursor = "chainhead"

    return RealtimeCursorSource(
        tracker,
        start_cursor=start_cursor,
        observability=observability,
    )
