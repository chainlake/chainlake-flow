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
) -> Panel:
    snap = progress.snapshot()
    bar = _render_progress_bar(snap["progress"], width=48)
    last_minute_messages = snap["last_minute_messages"]
    latest_sample = progress.recent_samples[-1] if progress.recent_samples else None
    phase_timings = latest_sample.phase_timings if latest_sample is not None else snap.get("last_phase_timings", {})
    messages_table = Table(box=box.SIMPLE_HEAVY, expand=True)
    messages_table.add_column("SOURCE", style="bold", no_wrap=True)
    messages_table.add_column("no. messages in\nthe last batch", justify="right")
    messages_table.add_column("messages in\nthe last minute", justify="right")
    messages_table.add_column("no. batches\nsince start", justify="right")
    messages_table.add_row(
        "Values",
        _format_int(snap["last_messages"]),
        _format_int(last_minute_messages),
        _format_int(snap["completed"]),
    )

    lag_table = Table(box=box.SIMPLE_HEAVY, expand=True)
    lag_table.add_column("LATENCY / LAG", style="bold", no_wrap=True)
    lag_table.add_column(Text("e2e latency [ms]", no_wrap=True), justify="right", no_wrap=True)
    lag_table.add_column(
        Text("ingestion lag [cursors]", no_wrap=True),
        justify="right",
        no_wrap=True,
    )
    lag_table.add_row(
        "Values",
        _format_ms_short(
            latest_sample.event_to_kafka_ms
            if latest_sample is not None and latest_sample.event_to_kafka_ms is not None
            else snap["last_latency_ms"]
        ),
        _format_int(snap["remaining"]),
    )

    cursor_detail_table = Table(box=box.SIMPLE_HEAVY, expand=True)
    cursor_detail_table.add_column("CURSOR DRILLDOWN", style="bold", no_wrap=True)
    cursor_detail_table.add_column(Text("value", no_wrap=True), justify="right", no_wrap=True)
    if latest_sample is not None:
        cursor_detail_table.add_row("cursor", str(latest_sample.cursor))
        cursor_detail_table.add_row("messages", _format_int(latest_sample.message_count))
        cursor_detail_table.add_row("completed at", _format_epoch_ms(int(latest_sample.completed_at)) if latest_sample.completed_at is not None else "n/a")
        cursor_detail_table.add_row("event ts", _format_epoch_ms(latest_sample.event_timestamp_ms))
        cursor_detail_table.add_row("ingest ts", _format_epoch_ms(latest_sample.ingest_timestamp_ms))
        cursor_detail_table.add_row("kafka append ts", _format_epoch_ms(latest_sample.kafka_append_timestamp_ms))
        cursor_detail_table.add_row("event -> ingest", _format_ms_short(latest_sample.event_to_ingest_ms))
        cursor_detail_table.add_row("ingest -> kafka", _format_ms_short(latest_sample.ingest_to_kafka_ms))
        cursor_detail_table.add_row("e2e", _format_ms_short(latest_sample.event_to_kafka_ms))
        cursor_detail_table.add_row("delivery wait", _format_ms_short(latest_sample.delivery_wait_ms))
        cursor_detail_table.add_row(
            "delivery messages",
            _format_int(latest_sample.cursor_timings.get("delivery_message_count") if isinstance(latest_sample.cursor_timings, dict) else None),
        )
        cursor_detail_table.add_row("checkpoint ms", _format_ms_short(latest_sample.cursor_timings.get("checkpoint_ms") if isinstance(latest_sample.cursor_timings, dict) else None))
        cursor_detail_table.add_row(
            "checkpoint delivery wait",
            _format_ms_short(latest_sample.cursor_timings.get("checkpoint_delivery_wait_ms") if isinstance(latest_sample.cursor_timings, dict) else None),
        )
        cursor_detail_table.add_row(
            "wall clock",
            _format_ms_short(latest_sample.cursor_timings.get("wall_clock_ms") if isinstance(latest_sample.cursor_timings, dict) else None),
        )
    else:
        cursor_detail_table.add_row("Values", "n/a")

    recent_table = Table(box=box.SIMPLE_HEAVY, expand=True)
    recent_table.add_column("RECENT CURSORS", style="bold", no_wrap=True)
    recent_table.add_column(Text("cursor", no_wrap=True), justify="right", no_wrap=True)
    recent_table.add_column(Text("e2e", no_wrap=True), justify="right", no_wrap=True)
    recent_table.add_column(Text("event->kafka", no_wrap=True), justify="right", no_wrap=True)
    recent_table.add_column(Text("sink delivery", no_wrap=True), justify="right", no_wrap=True)
    recent_table.add_column(Text("messages", no_wrap=True), justify="right", no_wrap=True)
    recent_samples = list(progress.recent_samples)[-6:]
    for sample in recent_samples:
        recent_table.add_row(
            "Values",
            str(sample.cursor),
            _format_ms_short(sample.event_to_kafka_ms if sample.event_to_kafka_ms is not None else sample.latency_ms),
            _format_ms_short(sample.event_to_kafka_ms),
            _format_ms_short(sample.delivery_wait_ms),
            _format_int(sample.message_count),
        )
    if not recent_samples:
        recent_table.add_row("Values", "n/a", "n/a", "n/a", "n/a", "n/a")

    phase_table = Table(box=box.SIMPLE_HEAVY, expand=True)
    phase_table.add_column("PHASE TIMINGS", style="bold", no_wrap=True)
    phase_table.add_column(Text("ms", no_wrap=True), justify="right", no_wrap=True)
    for label, key in (
        ("fetch", "fetch_ms"),
        ("rpc requests", "rpc_requests"),
        ("rpc queue avg", "rpc_queue_ms"),
        ("rpc avg", "rpc_ms"),
        ("rpc min", "rpc_min_ms"),
        ("rpc max", "rpc_max_ms"),
        ("process", "process_ms"),
        ("enrich", "enrich_ms"),
        ("sink enqueue", "sink_enqueue_ms"),
        ("sink delivery", "sink_delivery_ms"),
        ("sink total", "sink_ms"),
        ("checkpoint", "checkpoint_ms"),
        ("e2e", "e2e_ms"),
    ):
        value = phase_timings.get(key)
        if key == "rpc_requests":
            phase_table.add_row(label, str(value if value is not None else "n/a"))
        else:
            phase_table.add_row(label, _format_ms_short(value))

    logs = Table.grid(expand=True)
    logs.add_column(style="dim", width=22, no_wrap=True)
    logs.add_column(style="bold", width=8)
    logs.add_column(ratio=1)
    recent_logs = [record for record in logger.recent(10) if record["level"] == "info"]
    for record in recent_logs:
        fields = record["fields"]
        detail_bits = []
        for key in ("cursor", "entity", "stage", "payload", "error", "latency_ms", "ingestion_lag"):
            value = fields.get(key)
            if value is None:
                continue
            detail_bits.append(f"{key}={value}")
        detail = " ".join(detail_bits)
        logs.add_row(
            time.strftime("[%m/%d/%y %H:%M:%S]", time.localtime(record["time"])),
            record["level"].upper(),
            _truncate(f"{record['message']} {detail}".strip(), 120),
        )

    if not recent_logs:
        logs.add_row("-", "INFO", "waiting for benchmark activity...")
    while len(recent_logs) < 10:
        logs.add_row("", "", "")
        recent_logs.append(None)

    footer = Table.grid(expand=True)
    footer.add_column(ratio=1)
    footer.add_column(justify="right", no_wrap=True)
    footer.add_row(
        f"[dim]progress[/dim] {bar}  {snap['completed']}/{snap['total_cursors']} ({_format_percent(snap['progress'])})  "
        f"[dim]elapsed[/dim] {_format_duration(snap['elapsed_sec'])}  "
        f"[dim]eta[/dim] {_format_duration(snap['eta_sec'])}",
        f"[dim]mode[/dim] {mode}  [dim]sink[/dim] {sink}  [dim]eos[/dim] {'on' if eos_enabled else 'off'}  [dim]window[/dim] {window}",
    )

    return Panel(
        Group(
            "",
            Columns(
                [messages_table, Group(lag_table, "", cursor_detail_table)],
                expand=True,
                equal=True,
                padding=(0, 2),
            ),
            "",
            Columns(
                [phase_table, recent_table],
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
