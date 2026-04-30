from __future__ import annotations

from opentelemetry.metrics import Observation


_UNSET = object()


class WatermarkMetrics:
    def __init__(self, meter=None, *, attributes: dict[str, str] | None = None):
        self._attributes = dict(attributes or {})
        self._commit_cursor = None
        self._gap_count = 0
        self._oldest_gap = None
        self._commit_delay = None

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

    def update(
        self,
        *,
        commit_cursor: int | None | object = _UNSET,
        gap_count: int | None = None,
        oldest_gap: int | None | object = _UNSET,
        commit_delay: int | None | object = _UNSET,
    ) -> None:
        if commit_cursor is not _UNSET:
            self._commit_cursor = commit_cursor
        if gap_count is not None:
            self._gap_count = gap_count
        if oldest_gap is not _UNSET:
            self._oldest_gap = oldest_gap
        if commit_delay is not _UNSET:
            self._commit_delay = commit_delay

    def snapshot(self) -> dict[str, int | None]:
        return {
            "commit_cursor": self._commit_cursor,
            "gap_count": self._gap_count,
            "oldest_gap": self._oldest_gap,
            "commit_delay": self._commit_delay,
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
