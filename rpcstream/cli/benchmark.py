from __future__ import annotations

import asyncio
import json
import math
import os
import statistics
import sys
import time
from collections import defaultdict
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import select
import termios
import tty

import typer
from confluent_kafka import Producer
from rich import box
from rich.console import Console, Group
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from rpcstream.adapters import build_chain_adapter
from rpcstream.client.jsonrpc import JsonRpcClient
from rpcstream.config.loader import load_pipeline_config
from rpcstream.config.overrides import apply_runtime_overrides
from rpcstream.config.builder import build_erpc_endpoint
from rpcstream.config.resolver import resolve
from rpcstream.ingestion.engine import IngestionEngine
from rpcstream.planner.cursor_source import BackfillCursorSource
from rpcstream.scheduler.adaptive import AdaptiveRpcScheduler
from rpcstream.sinks.blackhole import BlackholeSink
from rpcstream.sinks.kafka.producer import KafkaWriter
from rpcstream.state.checkpoint import build_checkpoint_identity
from rpcstream.runtime.observability.provider import build_observability


@dataclass
class BenchmarkSample:
    cursor: int
    latency_ms: float
    message_count: int


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
        latencies = [sample.latency_ms for sample in self.samples]
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
                "fields": {
                    key: value
                    for key, value in kwargs.items()
                    if value is not None
                },
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
        self.latencies: list[float] = []
        self.last_cursor: int | None = None
        self.last_latency_ms: float | None = None
        self.last_messages: int = 0
        self.last_recorded_messages = 0
        self.last_refresh_completed = 0
        self.last_refresh_messages = 0

    def record_completion(self, cursor: int, *, latency_ms: float, total_messages: int) -> int:
        self.completed += 1
        delta_messages = max(total_messages - self.last_recorded_messages, 0)
        self.total_messages = total_messages
        self.latencies.append(latency_ms)
        self.last_cursor = cursor
        self.last_latency_ms = latency_ms
        self.last_messages = delta_messages
        self.last_recorded_messages = total_messages
        return delta_messages

    def snapshot(self) -> dict[str, Any]:
        now = time.perf_counter()
        elapsed = now - self.started_at
        since_refresh = max(now - self.last_refresh_at, 1e-6)
        completed_delta = self.completed - self.last_refresh_completed
        messages_delta = self.total_messages - self.last_refresh_messages
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
            "last_cursor": self.last_cursor,
            "last_latency_ms": self.last_latency_ms,
            "last_messages": self.last_messages,
            "avg_latency_ms": avg,
            "min_latency_ms": minimum,
            "max_latency_ms": maximum,
            "p95_latency_ms": p95,
            "total_messages": self.total_messages,
        }

    def mark_refreshed(self):
        self.last_refresh_at = time.perf_counter()
        self.last_refresh_completed = self.completed
        self.last_refresh_messages = self.total_messages


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


class NoopWatermarkManager:
    def __init__(self, runtime, sink_topic: str = "benchmark.commit_watermark"):
        self.identity = build_checkpoint_identity(runtime)
        self.topic = sink_topic
        self.state_topic = sink_topic.replace("commit_watermark", "cursor_state")
        self.cursor = None

    async def start(self):
        return None

    async def stop(self, status: str | None = None):
        return None

    async def mark_emitted(self, cursor: int):
        return None

    async def mark_completed(self, cursor: int):
        self.cursor = cursor
        return cursor

    async def mark_failed(self, cursor: int, error: str | None = None):
        return None

    async def requires_cursor_state(self, cursor: int) -> bool:
        return False

    async def preview_completed(self, cursor: int):
        return cursor

    def update_commit_delay(self, delay):
        return None


class CountingSink:
    def __init__(self, sink):
        self.sink = sink
        self.message_count = 0
        self.batch_count = 0
        self.transaction_count = 0
        self.topic_counts: dict[str, int] = defaultdict(int)

    async def start(self):
        return await self.sink.start()

    async def close(self):
        return await self.sink.close()

    async def send(self, topic, rows, wait_delivery=False):
        self.batch_count += 1
        self.message_count += len(rows)
        self.topic_counts[topic] += len(rows)
        return await self.sink.send(topic, rows, wait_delivery=wait_delivery)

    async def send_transaction(self, topic_rows):
        self.transaction_count += 1
        for topic, rows in topic_rows:
            self.message_count += len(rows)
            self.topic_counts[topic] += len(rows)
        return await self.sink.send_transaction(topic_rows)


def _default_config_path() -> str:
    return "pipeline.yaml"


def _parse_entities(values: list[str] | None) -> list[str] | None:
    if not values:
        return None

    entities: list[str] = []
    seen: set[str] = set()
    for value in values:
        for part in str(value).split(","):
            entity = part.strip()
            if not entity or entity in seen:
                continue
            seen.add(entity)
            entities.append(entity)
    return entities or None


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


def _render_progress_bar(progress: float, width: int = 36) -> str:
    progress = max(0.0, min(progress, 1.0))
    filled = int(width * progress)
    empty = width - filled
    return "█" * filled + "░" * empty


def _render_benchmark_dashboard(
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
    source_label = "CursorSource-0"
    sink_label = "KafkaWriter-0" if sink == "kafka" else "BlackholeSink-0"
    last_minute_messages = snap["last_sec_messages"] * 60 if snap["last_sec_messages"] is not None else None
    messages_table = Table(box=box.SIMPLE_HEAVY, expand=True)
    messages_table.add_column("MESSAGE", style="bold", no_wrap=True)
    messages_table.add_column("no. messages in\nthe last minibatch", justify="right")
    messages_table.add_column("in the last\nminute", justify="right")
    messages_table.add_column("since start", justify="right")
    messages_table.add_column("operator", justify="center", no_wrap=True)
    messages_table.add_column("latency to wall clock [ms]", justify="right")
    messages_table.add_column("lag to input [ms]", justify="right")
    messages_table.add_row(
        source_label,
        _format_int(snap["last_messages"]),
        _format_int(int(last_minute_messages)) if last_minute_messages is not None else "n/a",
        _format_int(snap["completed"]),
        "input",
        _format_ms_short(snap["last_latency_ms"]),
        _format_int(snap["remaining"]),
    )
    messages_table.add_row(
        sink_label,
        _format_int(snap["last_messages"]),
        _format_int(int(last_minute_messages)) if last_minute_messages is not None else "n/a",
        _format_int(snap["total_messages"]),
        "output",
        _format_ms_short(snap["avg_latency_ms"]),
        _format_int(snap["remaining"]),
    )

    lag_table = Table(box=box.SIMPLE_HEAVY, expand=True)
    lag_table.add_column("LATENCY / LAG", style="bold", no_wrap=True)
    lag_table.add_column("latency to wall clock [ms]", justify="right")
    lag_table.add_column("lag to input [ms]", justify="right")
    lag_table.add_column("avg [ms]", justify="right")
    lag_table.add_column("min [ms]", justify="right")
    lag_table.add_column("max [ms]", justify="right")
    lag_table.add_column("p50 [ms]", justify="right")
    lag_table.add_column("p95 [ms]", justify="right")
    lag_table.add_column("p99 [ms]", justify="right")
    lag_table.add_row(
        source_label,
        _format_ms_short(snap["last_latency_ms"]),
        _format_int(snap["remaining"]),
        _format_ms_short(snap["avg_latency_ms"]),
        _format_ms_short(snap["min_latency_ms"]),
        _format_ms_short(snap["max_latency_ms"]),
        _format_ms_short(_percentile(sorted(progress.latencies), 50)),
        _format_ms_short(snap["p95_latency_ms"]),
        _format_ms_short(_percentile(sorted(progress.latencies), 99)),
    )
    lag_table.add_row(
        sink_label,
        _format_ms_short(snap["avg_latency_ms"]),
        _format_int(snap["remaining"]),
        _format_ms_short(snap["avg_latency_ms"]),
        _format_ms_short(snap["min_latency_ms"]),
        _format_ms_short(snap["max_latency_ms"]),
        _format_ms_short(_percentile(sorted(progress.latencies), 50)),
        _format_ms_short(snap["p95_latency_ms"]),
        _format_ms_short(_percentile(sorted(progress.latencies), 99)),
    )

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
        recent_logs.append(None)  # keep table height stable

    footer_lines = [
        f"[dim]progress[/dim] {bar}  {snap['completed']}/{snap['total_cursors']} ({_format_percent(snap['progress'])})  "
        f"[dim]elapsed[/dim] {_format_duration(snap['elapsed_sec'])}  "
        f"[dim]eta[/dim] {_format_duration(snap['eta_sec'])}",
    ]
    if snap["completed"] >= snap["total_cursors"] > 0:
        footer_lines.append("[bold green]benchmark complete[/bold green]  press ESC or Ctrl-C to exit")

    return Panel(
        Group(
            messages_table,
            "",
            lag_table,
            "",
            logs,
            "",
            *footer_lines,
        ),
        title="RPCSTREAM PROGRESS DASHBOARD",
        title_align="center",
        border_style="cyan",
    )


def _write_benchmark_output_file(summary: BenchmarkSummary, output_file: str) -> Path:
    output_path = Path(output_file).expanduser()
    if not output_path.suffix:
        output_path = output_path.with_suffix(".json")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(summary.to_dict(), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return output_path


def _wait_for_exit_keypress() -> None:
    if not sys.stdin.isatty() or os.name != "posix":
        return

    fd = sys.stdin.fileno()
    original = termios.tcgetattr(fd)
    try:
        tty.setraw(fd)
        while True:
            ready, _, _ = select.select([fd], [], [], 0.1)
            if not ready:
                continue
            ch = sys.stdin.read(1)
            if ch in ("\x1b", "\x03"):
                return
    except KeyboardInterrupt:
        return
    finally:
        termios.tcsetattr(fd, termios.TCSADRAIN, original)


async def _wait_for_head(tracker, timeout_sec: float) -> int:
    deadline = time.monotonic() + timeout_sec
    while time.monotonic() < deadline:
        head_cursor = tracker.get_head_cursor()
        if head_cursor is not None:
            return int(head_cursor)
        await asyncio.sleep(min(getattr(tracker, "poll_interval", 0.2), 0.2))
    raise TimeoutError(f"timed out waiting for chainhead after {timeout_sec} seconds")


async def _discover_chainhead(
    *,
    base_url: str,
    timeout_sec: int,
    max_retries: int,
    poll_interval: float,
    adapter,
    observability,
    logger,
    head_timeout_sec: float,
) -> int:
    probe_client = JsonRpcClient(
        base_url=base_url,
        timeout_sec=timeout_sec,
        max_retries=max_retries,
        logger=logger,
        observability=observability,
    )
    tracker = adapter.build_tracker(
        client=probe_client,
        poll_interval=poll_interval,
        logger=logger,
    )
    tracker_started = False
    try:
        await tracker.start()
        tracker_started = True
        return await _wait_for_head(tracker, head_timeout_sec)
    finally:
        if tracker_started:
            await tracker.stop()
        else:
            await probe_client.close()


async def _build_sink(
    *,
    runtime,
    adapter,
    observability,
    logger,
    sink_kind: str,
    eos_enabled: bool,
):
    sink_kind = sink_kind.lower().strip()
    if sink_kind == "blackhole":
        return CountingSink(BlackholeSink(logger=logger))

    if sink_kind != "kafka":
        raise ValueError("--sink must be either 'blackhole' or 'kafka'")

    producer = Producer(runtime.kafka.config)
    writer = KafkaWriter(
        producer=producer,
        id_calculator=adapter.build_event_id_calculator(),
        time_calculator=adapter.build_event_time_calculator(),
        logger=logger,
        config=runtime.kafka.streaming,
        producer_config=runtime.kafka.config,
        topic_maps=runtime.topic_map,
        protobuf_enabled=runtime.kafka.protobuf_enabled,
        schema_registry_url=runtime.kafka.schema_registry_url,
        protobuf_topic_schemas=adapter.build_protobuf_topic_schemas(
            topic_maps=runtime.topic_map,
            entities=runtime.entities,
        ),
        observability=observability,
        eos_enabled=eos_enabled,
        eos_init_timeout_sec=runtime.kafka.eos_init_timeout_sec,
    )
    return CountingSink(writer)


async def _run_benchmark_async(
    *,
    config_path: str,
    mode: str,
    sink: str,
    eos_enabled: bool | None,
    window: int,
    entity: list[str] | None,
    head_timeout_sec: float,
    output_file: str | None,
) -> BenchmarkSummary:
    raw_config = load_pipeline_config(config_path)
    adapter = build_chain_adapter(raw_config.chain.type)
    observability = build_observability(raw_config.observability, raw_config.pipeline.name or "benchmark")
    logger = BenchmarkLogBuffer()
    await observability.start()

    try:
        head_cursor = await _discover_chainhead(
            base_url=build_erpc_endpoint(raw_config),
            timeout_sec=raw_config.erpc.timeout_sec,
            max_retries=raw_config.erpc.max_retries,
            poll_interval=raw_config.tracker.poll_interval,
            adapter=adapter,
            observability=observability,
            logger=logger,
            head_timeout_sec=head_timeout_sec,
        )
        start_cursor = head_cursor - window + 1
        if start_cursor < 0:
            raise ValueError(f"window={window} is larger than the current chainhead {head_cursor}")

        benchmark_config = apply_runtime_overrides(
            raw_config,
            mode="backfill",
            from_value=start_cursor,
            to_value=head_cursor,
            entities=_parse_entities(entity),
            eos_enabled=eos_enabled,
        )
        runtime = resolve(benchmark_config, adapter=adapter)
        run_client = JsonRpcClient(
            base_url=runtime.client.base_url,
            timeout_sec=runtime.client.timeout_sec,
            max_retries=runtime.client.max_retries,
            logger=logger,
            observability=observability,
        )
        sink_obj = await _build_sink(
            runtime=runtime,
            adapter=adapter,
            observability=observability,
            logger=logger,
            sink_kind=sink,
            eos_enabled=runtime.kafka.eos_enabled,
        )
        watermark_manager = NoopWatermarkManager(runtime)
        engine = IngestionEngine(
            fetcher=adapter.build_fetcher(
                scheduler=AdaptiveRpcScheduler(
                    run_client,
                    initial_inflight=runtime.scheduler.initial_inflight,
                    max_inflight=runtime.scheduler.max_inflight,
                    min_inflight=runtime.scheduler.min_inflight,
                    latency_target_ms=runtime.scheduler.latency_target_ms,
                    logger=logger,
                    observability=observability,
                ),
                entities=getattr(runtime, "internal_entities", runtime.entities),
                logger=logger,
                tracker=None,
            ),
            processors=adapter.build_processors(entities=getattr(runtime, "internal_entities", runtime.entities)),
            enricher=adapter.build_enricher(),
            sink=sink_obj,
            topics=runtime.topic_map.main,
            dlq_topic=None,
            chain=runtime.chain,
            pipeline=runtime.pipeline,
            max_retry=runtime.client.max_retries,
            concurrency=runtime.engine.concurrency,
            logger=logger,
            observability=observability,
            watermark_manager=watermark_manager,
            checkpoint_reader=None,
            eos_enabled=runtime.kafka.eos_enabled,
        )

        cursor_source = BackfillCursorSource(start=start_cursor, end=head_cursor)
        samples: list[BenchmarkSample] = []
        progress = BenchmarkProgress(total_cursors=window)
        progress_lock = asyncio.Lock()
        dashboard_stop = asyncio.Event()
        dashboard_console = Console(file=sys.stderr, force_terminal=True, color_system="auto")
        benchmark_mode = mode.strip().lower()

        async def refresh_dashboard(live: Live):
            while not dashboard_stop.is_set():
                live.update(
                    _render_benchmark_dashboard(
                        mode=benchmark_mode,
                        sink=sink.lower().strip(),
                        eos_enabled=runtime.kafka.eos_enabled,
                        window=window,
                        head_cursor=head_cursor,
                        progress=progress,
                        logger=logger,
                    )
                )
                live.refresh()
                progress.mark_refreshed()
                await asyncio.sleep(1.0)

        total_started = time.perf_counter()

        await sink_obj.start()
        with Live(
            _render_benchmark_dashboard(
                mode=benchmark_mode,
                sink=sink.lower().strip(),
                eos_enabled=runtime.kafka.eos_enabled,
                window=window,
                head_cursor=head_cursor,
                progress=progress,
                logger=logger,
            ),
            console=dashboard_console,
            screen=True,
            auto_refresh=False,
            transient=False,
        ) as live:
            dashboard_task = asyncio.create_task(refresh_dashboard(live))
            try:
                if benchmark_mode == "serial":
                    while True:
                        cursor = await cursor_source.next_cursor()
                        if cursor is None:
                            break

                        cursor_started = time.perf_counter()
                        success, delivery_futures, expected_watermark = await engine._run_one(cursor)
                        await engine._finalize_checkpoint(
                            cursor,
                            success,
                            delivery_futures,
                            expected_watermark=expected_watermark,
                        )
                        latency_ms = (time.perf_counter() - cursor_started) * 1000
                        async with progress_lock:
                            delta_messages = progress.record_completion(
                                int(cursor),
                                latency_ms=latency_ms,
                                total_messages=sink_obj.message_count,
                            )

                        samples.append(
                            BenchmarkSample(
                                cursor=int(cursor),
                                latency_ms=latency_ms,
                                message_count=delta_messages,
                            )
                        )
                elif benchmark_mode == "concurrent":
                    worker_count = 1 if runtime.kafka.eos_enabled else max(1, runtime.engine.concurrency)
                    queue_size = max(1, max(runtime.scheduler.max_inflight, runtime.engine.concurrency) * 2)
                    queue: asyncio.Queue[tuple[int, float] | None] = asyncio.Queue(maxsize=queue_size)

                    async def producer():
                        try:
                            while True:
                                cursor = await cursor_source.next_cursor()
                                if cursor is None:
                                    break
                                await queue.put((int(cursor), time.perf_counter()))
                        finally:
                            for _ in range(worker_count):
                                await queue.put(None)

                    async def worker():
                        while True:
                            item = await queue.get()
                            if item is None:
                                break
                            cursor, cursor_started = item
                            success, delivery_futures, expected_watermark = await engine._run_one(cursor)
                            await engine._finalize_checkpoint(
                                cursor,
                                success,
                                delivery_futures,
                                expected_watermark=expected_watermark,
                            )
                            latency_ms = (time.perf_counter() - cursor_started) * 1000
                            async with progress_lock:
                                delta_messages = progress.record_completion(
                                    cursor,
                                    latency_ms=latency_ms,
                                    total_messages=sink_obj.message_count,
                                )
                                samples.append(
                                    BenchmarkSample(
                                        cursor=cursor,
                                        latency_ms=latency_ms,
                                        message_count=delta_messages,
                                    )
                                )

                    workers = [asyncio.create_task(worker()) for _ in range(worker_count)]
                    await producer()
                    await asyncio.gather(*workers)
                else:
                    raise ValueError("--mode must be either 'serial' or 'concurrent'")
            finally:
                dashboard_stop.set()
                await dashboard_task
                live.update(
                    _render_benchmark_dashboard(
                        mode=benchmark_mode,
                        sink=sink.lower().strip(),
                        eos_enabled=runtime.kafka.eos_enabled,
                        window=window,
                        head_cursor=head_cursor,
                        progress=progress,
                        logger=logger,
                    )
                )
                live.refresh()
                if dashboard_console.is_terminal:
                    await asyncio.to_thread(_wait_for_exit_keypress)
        await sink_obj.close()
        total_elapsed_sec = time.perf_counter() - total_started
        total_messages = sink_obj.message_count
        total_cursors = len(samples)
        summary = BenchmarkSummary(
            mode=benchmark_mode,
            chain_name=runtime.chain.name,
            network=runtime.chain.network,
            sink=sink.lower().strip(),
            eos_enabled=runtime.kafka.eos_enabled,
            start_cursor=start_cursor,
            end_cursor=head_cursor,
            total_cursors=total_cursors,
            total_messages=total_messages,
            total_elapsed_sec=total_elapsed_sec,
            samples=samples,
        )
        if output_file:
            _write_benchmark_output_file(summary, output_file)
        return summary
    finally:
        if "run_client" in locals():
            await run_client.close()
        await observability.shutdown()


def benchmark(
    config_path: str = typer.Option(
        None,
        "--config",
        help="Path to pipeline.yaml.",
    ),
    sink: str = typer.Option(
        "blackhole",
        "--sink",
        help="Benchmark sink. Use blackhole for RPC-only measurement or kafka for producer ack timing.",
    ),
    eos_enabled: bool = typer.Option(
        False,
        "--eos-enabled/--no-eos-enabled",
        help="Toggle Kafka EOS for the benchmark run.",
    ),
    window: int = typer.Option(
        1000,
        "--window",
        help="Number of cursors to include ending at chainhead. A window of 1000 runs from head-999 through head.",
    ),
    entity: list[str] | None = typer.Option(
        None,
        "--entity",
        help="Optional entity override using comma-separated values or repeated --entity flags.",
    ),
    head_timeout_sec: float = typer.Option(
        30.0,
        "--head-timeout-sec",
        help="Timeout while waiting for the current chainhead before the benchmark window is computed.",
    ),
    mode: str = typer.Option(
        "concurrent",
        "--mode",
        help="Benchmark execution mode: serial or concurrent. Concurrent is best for no-EOS throughput tests.",
    ),
    output_file: str | None = typer.Option(
        None,
        "--output-file",
        help="Optional JSON output file. If set without a suffix, .json is appended.",
    ),
) -> None:
    """Benchmark ingestion from chainhead-window to chainhead and show a live dashboard."""
    config_path = config_path or _default_config_path()
    try:
        summary = asyncio.run(
            _run_benchmark_async(
                config_path=config_path,
                sink=sink,
                mode=mode,
                eos_enabled=eos_enabled,
                window=window,
                entity=entity,
                head_timeout_sec=head_timeout_sec,
                output_file=output_file,
            )
        )
    except KeyboardInterrupt:
        raise typer.Exit(130) from None
    except Exception as exc:
        typer.secho(str(exc), fg=typer.colors.RED, err=True)
        raise typer.Exit(1) from exc

    if output_file:
        output_path = Path(output_file).expanduser()
        if not output_path.suffix:
            output_path = output_path.with_suffix(".json")
        typer.echo(f"wrote benchmark results to {output_path}")
