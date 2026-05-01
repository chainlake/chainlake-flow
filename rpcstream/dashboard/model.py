from __future__ import annotations

import math
import statistics
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any


@dataclass
class BenchmarkSample:
    cursor: int
    latency_ms: float
    message_count: int
    completed_at: float | None = None
    phase_timings: dict[str, float] = field(default_factory=dict)
    event_timestamp_ms: int | None = None
    ingest_timestamp_ms: int | None = None
    kafka_append_timestamp_ms: int | None = None
    event_to_ingest_ms: float | None = None
    ingest_to_kafka_ms: float | None = None
    event_to_kafka_ms: float | None = None
    delivery_wait_ms: float | None = None
    head_observed_at_ms: int | None = None
    cursor_emitted_at_ms: int | None = None
    head_observed_to_emit_ms: float | None = None
    head_observed_lag_ms: float | None = None
    tracker_poll_latency_ms: float | None = None
    cursor_timings: dict[str, float | int | None] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "cursor": self.cursor,
            "latency_ms": self.latency_ms,
            "message_count": self.message_count,
            "completed_at": self.completed_at,
            "phase_timings": dict(self.phase_timings),
            "event_timestamp_ms": self.event_timestamp_ms,
            "ingest_timestamp_ms": self.ingest_timestamp_ms,
            "kafka_append_timestamp_ms": self.kafka_append_timestamp_ms,
            "event_to_ingest_ms": self.event_to_ingest_ms,
            "ingest_to_kafka_ms": self.ingest_to_kafka_ms,
            "event_to_kafka_ms": self.event_to_kafka_ms,
            "delivery_wait_ms": self.delivery_wait_ms,
            "head_observed_at_ms": self.head_observed_at_ms,
            "cursor_emitted_at_ms": self.cursor_emitted_at_ms,
            "head_observed_to_emit_ms": self.head_observed_to_emit_ms,
            "head_observed_lag_ms": self.head_observed_lag_ms,
            "tracker_poll_latency_ms": self.tracker_poll_latency_ms,
            "cursor_timings": dict(self.cursor_timings),
        }


@dataclass
class BenchmarkSummary:
    mode: str
    chain_name: str
    network: str
    sink: str
    eos_enabled: bool
    start_cursor: int
    end_cursor: int
    total_cursors: int
    total_messages: int
    total_elapsed_sec: float
    samples: list[BenchmarkSample]

    def to_dict(self) -> dict[str, Any]:
        latencies = [
            sample.event_to_kafka_ms if sample.event_to_kafka_ms is not None else sample.latency_ms
            for sample in self.samples
        ]
        latencies.sort()
        avg = statistics.mean(latencies) if latencies else None
        minimum = latencies[0] if latencies else None
        maximum = latencies[-1] if latencies else None
        p50 = _percentile(latencies, 50)
        p95 = _percentile(latencies, 95)
        p99 = _percentile(latencies, 99)
        blocks_per_sec = self.total_cursors / self.total_elapsed_sec if self.total_elapsed_sec > 0 else None
        messages_per_sec = self.total_messages / self.total_elapsed_sec if self.total_elapsed_sec > 0 else None

        return {
            "mode": self.mode,
            "chain_name": self.chain_name,
            "network": self.network,
            "sink": self.sink,
            "eos_enabled": self.eos_enabled,
            "start_cursor": self.start_cursor,
            "end_cursor": self.end_cursor,
            "total_cursors": self.total_cursors,
            "total_messages": self.total_messages,
            "total_elapsed_sec": self.total_elapsed_sec,
            "blocks_per_sec": blocks_per_sec,
            "messages_per_sec": messages_per_sec,
            "latency_ms": {
                "avg": avg,
                "min": minimum,
                "max": maximum,
                "p50": p50,
                "p95": p95,
                "p99": p99,
            },
            "samples": [sample.to_dict() for sample in self.samples],
        }


class BenchmarkLogBuffer:
    def __init__(self, max_records: int = 100):
        self.records = deque(maxlen=max_records)

    def debug(self, message, **kwargs):
        self._append("debug", message, **kwargs)

    def info(self, message, **kwargs):
        self._append("info", message, **kwargs)

    def warn(self, message, **kwargs):
        self._append("warn", message, **kwargs)

    def error(self, message, **kwargs):
        self._append("error", message, **kwargs)

    def _append(self, level: str, message: str, **kwargs):
        if level == "debug":
            return
        self.records.append(
            {
                "time": time.time(),
                "level": level,
                "message": message,
                "fields": {key: value for key, value in kwargs.items() if value is not None},
            }
        )

    def recent(self, limit: int = 5) -> list[dict[str, Any]]:
        return list(self.records)[-limit:]


class BenchmarkProgress:
    def __init__(self, total_cursors: int):
        self.total_cursors = total_cursors
        self.started_at = time.perf_counter()
        self.last_refresh_at = self.started_at
        self.completed = 0
        self.total_messages = 0
        self.message_history: deque[tuple[float, int]] = deque()
        self.latencies: list[float] = []
        self.last_cursor: int | None = None
        self.last_latency_ms: float | None = None
        self.last_messages: int = 0
        self.last_phase_timings: dict[str, float] = {}
        self.last_recorded_messages = 0
        self.last_refresh_completed = 0
        self.last_refresh_messages = 0
        self.recent_samples: deque[BenchmarkSample] = deque(maxlen=20)

    def record_completion(
        self,
        cursor: int,
        *,
        latency_ms: float,
        total_messages: int,
        phase_timings: dict[str, float] | None = None,
        sample: BenchmarkSample | None = None,
    ) -> int:
        self.completed += 1
        delta_messages = max(total_messages - self.last_recorded_messages, 0)
        self.total_messages = total_messages
        self.latencies.append(latency_ms)
        now = time.perf_counter()
        self.message_history.append((now, delta_messages))
        self._prune_message_history(now)
        self.last_cursor = cursor
        self.last_latency_ms = latency_ms
        self.last_messages = delta_messages
        self.last_phase_timings = dict(phase_timings or {})
        self.last_recorded_messages = total_messages
        if sample is not None:
            self.recent_samples.append(sample)
        return delta_messages

    def snapshot(self) -> dict[str, Any]:
        now = time.perf_counter()
        elapsed = now - self.started_at
        since_refresh = max(now - self.last_refresh_at, 1e-6)
        completed_delta = self.completed - self.last_refresh_completed
        messages_delta = self.total_messages - self.last_refresh_messages
        last_minute_messages = self._messages_in_last_seconds(now, 60.0)
        rate = self.completed / elapsed if elapsed > 0 else None
        msg_rate = self.total_messages / elapsed if elapsed > 0 else None
        remaining = max(self.total_cursors - self.completed, 0)
        eta = remaining / rate if rate and rate > 0 else None
        latencies = sorted(self.latencies)
        p95 = _percentile(latencies, 95)
        avg = statistics.mean(latencies) if latencies else None
        progress = (self.completed / self.total_cursors) if self.total_cursors > 0 else 1.0
        last_sec_completed = completed_delta / since_refresh if since_refresh > 0 else None
        last_sec_messages = messages_delta / since_refresh if since_refresh > 0 else None
        minimum = latencies[0] if latencies else None
        maximum = latencies[-1] if latencies else None

        return {
            "progress": progress,
            "completed": self.completed,
            "total_cursors": self.total_cursors,
            "remaining": remaining,
            "elapsed_sec": elapsed,
            "eta_sec": eta,
            "rate_cursors": rate,
            "rate_messages": msg_rate,
            "last_sec_completed": last_sec_completed,
            "last_sec_messages": last_sec_messages,
            "last_minute_messages": last_minute_messages,
            "last_cursor": self.last_cursor,
            "last_latency_ms": self.last_latency_ms,
            "last_messages": self.last_messages,
            "last_phase_timings": dict(self.last_phase_timings),
            "avg_latency_ms": avg,
            "min_latency_ms": minimum,
            "max_latency_ms": maximum,
            "p95_latency_ms": p95,
            "total_messages": self.total_messages,
            "recent_samples": [sample.to_dict() for sample in self.recent_samples],
        }

    def mark_refreshed(self):
        self.last_refresh_at = time.perf_counter()
        self.last_refresh_completed = self.completed
        self.last_refresh_messages = self.total_messages

    def _prune_message_history(self, now: float, window_sec: float = 60.0) -> None:
        cutoff = now - window_sec
        while self.message_history and self.message_history[0][0] < cutoff:
            self.message_history.popleft()

    def _messages_in_last_seconds(self, now: float, window_sec: float) -> int:
        self._prune_message_history(now, window_sec=window_sec)
        return sum(count for _ts, count in self.message_history)


def _percentile(sorted_values: list[float], pct: float) -> float | None:
    if not sorted_values:
        return None
    if len(sorted_values) == 1:
        return sorted_values[0]
    position = (len(sorted_values) - 1) * (pct / 100.0)
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return sorted_values[lower]
    lower_value = sorted_values[lower]
    upper_value = sorted_values[upper]
    return lower_value + ((upper_value - lower_value) * (position - lower))


__all__ = [
    "BenchmarkLogBuffer",
    "BenchmarkProgress",
    "BenchmarkSample",
    "BenchmarkSummary",
]
