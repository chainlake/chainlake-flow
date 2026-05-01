from __future__ import annotations

import json
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from rich import box
from rich.console import Group
from rich.columns import Columns
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from rpcstream.dashboard.model import BenchmarkLogBuffer, BenchmarkProgress, BenchmarkSummary


def _format_rate(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:.2f}/s"


def _format_duration(value: float | None) -> str:
    if value is None:
        return "n/a"
    if value < 60:
        return f"{value:.1f}s"
    minutes = int(value // 60)
    seconds = value - (minutes * 60)
    return f"{minutes}m{seconds:04.1f}s"


def _truncate(text: str, limit: int = 72) -> str:
    if len(text) <= limit:
        return text
    return text[: limit - 1] + "…"


def _format_ms(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:.2f}"


def _format_int(value: int | None) -> str:
    if value is None:
        return "n/a"
    return str(value)


def _format_percent(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value * 100:.1f}%"


def _format_ms_short(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:.1f}ms"


def _format_epoch_ms(value: int | None) -> str:
    if value is None:
        return "n/a"
    try:
        return datetime.fromtimestamp(value / 1000.0).strftime("%m/%d %H:%M:%S.%f")[:-3]
    except Exception:
        return str(value)


def _format_log_time(value: float | int | None) -> str:
    if value is None:
        return "n/a"
    try:
        return datetime.fromtimestamp(float(value)).strftime("%Y/%m/%d %H:%M:%S.%f")[:-3]
    except Exception:
        return str(value)


def _render_progress_bar(progress: float, width: int = 36) -> str:
    progress = max(0.0, min(progress, 1.0))
    filled = int(width * progress)
    empty = width - filled
    return "█" * filled + "░" * empty


def render_benchmark_dashboard(
    *,
    mode: str,
    sink: str,
    eos_enabled: bool,
    window: int,
    head_cursor: int,
    progress: BenchmarkProgress,
    logger: BenchmarkLogBuffer,
    scheduler=None,
) -> Panel:
    snap = progress.snapshot()
    bar = _render_progress_bar(snap["progress"], width=48)
    last_minute_messages = snap["last_minute_messages"]
    latest_sample = progress.recent_samples[-1] if progress.recent_samples else None
    cursor_detail_table = Table(box=box.SIMPLE_HEAVY, expand=True)
    cursor_detail_table.add_column("TIMESTAMPS", style="bold", no_wrap=True)
    cursor_detail_table.add_column(Text("value", no_wrap=True), justify="right", no_wrap=True)
    cursor_detail_table.add_column(Text("meaning", no_wrap=True), justify="left", no_wrap=True)
    if latest_sample is not None:
        cursor_detail_table.add_row("cursor", str(latest_sample.cursor), "current cursor being processed")
        cursor_detail_table.add_row("messages", _format_int(latest_sample.message_count), "rows emitted for this cursor")
        cursor_detail_table.add_row("chain event ts", _format_epoch_ms(latest_sample.event_timestamp_ms), "event timestamp from chain data")
        cursor_detail_table.add_row("chainhead observed at", _format_epoch_ms(latest_sample.head_observed_at_ms), "tracker first saw the new head")
        cursor_detail_table.add_row("cursor dispatched at", _format_epoch_ms(latest_sample.cursor_emitted_at_ms), "cursor handed to the engine")
        cursor_detail_table.add_row("ingest ts", _format_epoch_ms(latest_sample.ingest_timestamp_ms), "message entered Kafka writer")
        cursor_detail_table.add_row("kafka appends ts", _format_epoch_ms(latest_sample.kafka_append_timestamp_ms), "Kafka log append time")
        cursor_detail_table.add_row("completed at", _format_epoch_ms(int(latest_sample.completed_at)) if latest_sample.completed_at is not None else "n/a", "this cursor finished processing")
    else:
        cursor_detail_table.add_row("Values", "n/a", "n/a")

    latency_table = Table(box=box.SIMPLE_HEAVY, expand=True)
    latency_table.add_column("LATENCIES", style="bold", no_wrap=True)
    latency_table.add_column(Text("value", no_wrap=True), justify="right", no_wrap=True)
    latency_table.add_column(Text("meaning", no_wrap=True), justify="left", no_wrap=True)
    if latest_sample is not None:
        latency_table.add_row("chainhead poll lag", _format_ms_short(latest_sample.head_observed_lag_ms), "chain event ts to head observation lag")
        latency_table.add_row("chainhead -> dispatch", _format_ms_short(latest_sample.head_observed_to_emit_ms), "head observation to engine dispatch")
        latency_table.add_row("chainhead poll latency", _format_ms_short(latest_sample.tracker_poll_latency_ms), "tracker poll round-trip latency")
        latency_table.add_row("event -> ingest", _format_ms_short(latest_sample.event_to_ingest_ms), "chain event ts to ingest ts")
        latency_table.add_row("ingest -> kafka", _format_ms_short(latest_sample.ingest_to_kafka_ms), "ingest ts to kafka append ts")
        latency_table.add_row("e2e", _format_ms_short(latest_sample.event_to_kafka_ms), "chain event ts to kafka append ts")
        latency_table.add_row(
            "processing wall clock",
            _format_ms_short(latest_sample.cursor_timings.get("wall_clock_ms") if isinstance(latest_sample.cursor_timings, dict) else None),
            "total wall clock for this cursor",
        )
    else:
        latency_table.add_row("Values", "n/a", "n/a")

    recent_samples = list(progress.recent_samples)[-10:]
    recent_sample_by_cursor = {sample.cursor: sample for sample in recent_samples}
    recent_logs = [record for record in logger.recent(10) if record["level"] == "info"]
    logs = Table(box=box.SIMPLE_HEAVY, expand=True, title="LOGS", title_justify="center")
    logs.add_column(Text("time", no_wrap=True), justify="right", no_wrap=True)
    logs.add_column(Text("level", no_wrap=True), justify="right", no_wrap=True)
    logs.add_column(Text("cursor", no_wrap=True), justify="right", no_wrap=True)
    logs.add_column(Text("entity", no_wrap=True), justify="right", no_wrap=True)
    logs.add_column(Text("stage", no_wrap=True), justify="right", no_wrap=True)
    logs.add_column(Text("rpc_latency", no_wrap=True), justify="right", no_wrap=True)
    logs.add_column(Text("e2e", no_wrap=True), justify="right", no_wrap=True)
    logs.add_column(Text("ingestion_lag [cursors]", no_wrap=True), justify="right", no_wrap=True)
    logs.add_column(Text("message", no_wrap=True), justify="right", no_wrap=True)
    for record in recent_logs:
        fields = record["fields"]
        cursor_value = fields.get("cursor")
        cursor_display = "n/a"
        if cursor_value is not None:
            try:
                cursor_display = str(int(cursor_value))
            except (TypeError, ValueError):
                cursor_display = str(cursor_value)
        rpc_latency_value = fields.get("rpc_latency", fields.get("latency_ms"))
        sample = None
        if cursor_value is not None:
            try:
                sample = recent_sample_by_cursor.get(int(cursor_value))
            except (TypeError, ValueError):
                sample = None
        e2e_value = sample.event_to_kafka_ms if sample is not None and mode.strip().lower() == "realtime" else None
        ingestion_lag_value = fields.get("ingestion_lag")
        message_value = fields.get("message", fields.get("payload"))
        logs.add_row(
            _format_log_time(record["time"]),
            record["level"].upper(),
            cursor_display,
            str(fields.get("entity")) if fields.get("entity") is not None else "n/a",
            str(fields.get("stage", record["message"])),
            _format_ms_short(float(rpc_latency_value)) if isinstance(rpc_latency_value, (int, float)) else "n/a",
            _format_ms_short(float(e2e_value)) if isinstance(e2e_value, (int, float)) else "n/a",
            str(int(ingestion_lag_value)) if isinstance(ingestion_lag_value, (int, float)) else "n/a",
            _format_int(int(message_value)) if isinstance(message_value, (int, float)) else "n/a",
        )

    if not recent_logs:
        logs.add_row("-", "INFO", "n/a", "n/a", "waiting for benchmark activity...", "n/a", "n/a", "n/a", "n/a")
    while len(recent_logs) < 10:
        logs.add_row("", "", "", "", "", "", "", "", "")
        recent_logs.append(None)

    footer = Table.grid(expand=True)
    footer.add_column(ratio=1)
    footer.add_column(justify="right", no_wrap=True)
    footer.add_row(
        f"[dim]progress[/dim] {bar}  {snap['completed']}/{snap['total_cursors']} ({_format_percent(snap['progress'])})  "
        f"[dim]elapsed[/dim] {_format_duration(snap['elapsed_sec'])}  "
        f"[dim]eta[/dim] {_format_duration(snap['eta_sec'])}",
        f"[dim]mode[/dim] {mode}  [dim]sink[/dim] {sink}  [dim]eos[/dim] {'on' if eos_enabled else 'off'}  [dim]window[/dim] {window}  "
        f"[dim]cursors/s[/dim] {_format_rate(snap.get('rate_cursors'))}  "
        f"[dim]msgs/s[/dim] {_format_rate(snap.get('rate_messages'))}",
    )

    return Panel(
        Group(
            "",
            Columns(
                [cursor_detail_table, latency_table],
                expand=True,
                equal=True,
                padding=(0, 2),
            ),
            "",
            logs,
            "",
            footer,
            (
                "[bold green]benchmark complete[/bold green]  press ESC or Ctrl-C to exit"
                if snap["completed"] >= snap["total_cursors"] > 0
                else ""
            ),
        ),
        title="RPCSTREAM PROGRESS DASHBOARD",
        title_align="center",
        border_style="cyan",
    )


def write_benchmark_output_file(summary: BenchmarkSummary, output_file: str) -> Path:
    output_path = Path(output_file).expanduser()
    if not output_path.suffix:
        output_path = output_path.with_suffix(".json")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(summary.to_dict(), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return output_path


def _percentile(sorted_values: list[float], pct: float) -> float | None:
    if not sorted_values:
        return None
    if len(sorted_values) == 1:
        return sorted_values[0]
    position = (len(sorted_values) - 1) * (pct / 100.0)
    lower = int(position)
    upper = lower if position.is_integer() else lower + 1
    if lower == upper:
        return sorted_values[lower]
    lower_value = sorted_values[lower]
    upper_value = sorted_values[upper]
    return lower_value + ((upper_value - lower_value) * (position - lower))


__all__ = [
    "render_benchmark_dashboard",
    "write_benchmark_output_file",
]
