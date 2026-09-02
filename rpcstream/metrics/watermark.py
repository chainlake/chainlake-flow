from __future__ import annotations

from opentelemetry.metrics import Observation


_UNSET = object()


class _NoOpCounter:
    def add(self, *args, **kwargs):
        pass


class WatermarkMetrics:
    def __init__(self, meter=None, *, attributes: dict[str, str] | None = None):
        self._attributes = dict(attributes or {})
        self._commit_cursor = None
        self._gap_count = 0
        self._oldest_gap = None
        self._commit_delay = None
        self._backfill_start = None
        self._backfill_target = None
        self._cursor_failed_counter = _NoOpCounter()

        if meter is None:
            return

        meter.create_observable_gauge(
            "rpcstream_watermark_commit_cursor",
            callbacks=[self._observe_commit_cursor],
            description="Current committed contiguous watermark cursor.",
        )
        meter.create_observable_gauge(
            "rpcstream_watermark_gap_count",
            callbacks=[self._observe_gap_count],
            description="Number of unresolved watermark gaps.",
        )
        meter.create_observable_gauge(
            "rpcstream_watermark_oldest_gap",
            callbacks=[self._observe_oldest_gap],
            description="Oldest unresolved watermark gap cursor.",
        )
        meter.create_observable_gauge(
            "rpcstream_watermark_commit_delay",
            callbacks=[self._observe_commit_delay],
            description="Distance from the current commit watermark to chainhead or backfill target.",
        )
        # Bounded-backfill segment bounds, exported per instance ONLY when the
        # process is running a bounded backfill (pipeline.from/to numeric).
        # Real-time processes never call update(start_cursor=..., ...) so no
        # series is exported for them. They carry the exact same attribute set
        # as rpcstream_watermark_commit_cursor, so a PromQL expression like
        # (cursor - start) / (target - start) joins 1:1 per instance without
        # hardcoding ranges -- dashboards stay correct as segment splits and
        # the number of parallel instances change.
        meter.create_observable_gauge(
            "rpcstream_watermark_backfill_start",
            callbacks=[self._observe_backfill_start],
            description="Configured segment start cursor (pipeline.from) for this bounded backfill process.",
        )
        meter.create_observable_gauge(
            "rpcstream_watermark_backfill_target",
            callbacks=[self._observe_backfill_target],
            description="Configured segment end cursor (pipeline.to) for this bounded backfill process.",
        )

        # rpcstream_watermark_gap_count is a point-in-time gauge (current
        # size of the unresolved-cursor set) -- it nets new failures against
        # retries resolving old ones, so it can sit flat while failures are
        # actively happening underneath it. This counter is the missing
        # "rate of new watermark.cursor_failed events" signal, incremented
        # once per mark_failed() call regardless of whether the cursor was
        # already in the failed set.
        self._cursor_failed_counter = meter.create_counter(
            "rpcstream_watermark_cursor_failed_total",
            description="Count of mark_failed() calls (watermark.cursor_failed log events).",
        )

    def update(
        self,
        *,
        commit_cursor: int | None | object = _UNSET,
        gap_count: int | None = None,
        oldest_gap: int | None | object = _UNSET,
        commit_delay: int | None | object = _UNSET,
        start_cursor: int | None | object = _UNSET,
        target_cursor: int | None | object = _UNSET,
    ) -> None:
        if commit_cursor is not _UNSET:
            self._commit_cursor = commit_cursor
        if gap_count is not None:
            self._gap_count = gap_count
        if oldest_gap is not _UNSET:
            self._oldest_gap = oldest_gap
        if commit_delay is not _UNSET:
            self._commit_delay = commit_delay
        if start_cursor is not _UNSET:
            self._backfill_start = start_cursor
        if target_cursor is not _UNSET:
            self._backfill_target = target_cursor

    def record_cursor_failed(self) -> None:
        self._cursor_failed_counter.add(1, self._attributes)

    def snapshot(self) -> dict[str, int | None]:
        return {
            "commit_cursor": self._commit_cursor,
            "gap_count": self._gap_count,
            "oldest_gap": self._oldest_gap,
            "commit_delay": self._commit_delay,
            "backfill_start": self._backfill_start,
            "backfill_target": self._backfill_target,
        }

    def _observe_commit_cursor(self, _options):
        if self._commit_cursor is None:
            return []
        return [Observation(self._commit_cursor, self._attributes)]

    def _observe_gap_count(self, _options):
        return [Observation(self._gap_count, self._attributes)]

    def _observe_oldest_gap(self, _options):
        if self._oldest_gap is None:
            return []
        return [Observation(self._oldest_gap, self._attributes)]

    def _observe_commit_delay(self, _options):
        if self._commit_delay is None:
            return []
        return [Observation(self._commit_delay, self._attributes)]

    def _observe_backfill_start(self, _options):
        if self._backfill_start is None:
            return []
        return [Observation(self._backfill_start, self._attributes)]

    def _observe_backfill_target(self, _options):
        if self._backfill_target is None:
            return []
        return [Observation(self._backfill_target, self._attributes)]
