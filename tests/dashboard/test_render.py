from __future__ import annotations

from rich.console import Console

from rpcstream.dashboard.model import BenchmarkLogBuffer, BenchmarkProgress, BenchmarkSample
from rpcstream.dashboard.render import render_benchmark_dashboard


def test_benchmark_dashboard_does_not_render_processing_wall_clock():
    sample = BenchmarkSample(
        cursor=123,
        latency_ms=12.3,
        message_count=4,
        completed_at=1_700_000_000_000.0,
        phase_timings={"fetch_ms": 1.2, "process_ms": 2.3, "enrich_ms": 3.4},
        event_timestamp_ms=1_700_000_000_000,
        ingest_timestamp_ms=1_700_000_000_005,
        kafka_append_timestamp_ms=1_700_000_000_010,
        event_to_ingest_ms=5.0,
        ingest_to_kafka_ms=5.0,
        event_to_kafka_ms=10.0,
        delivery_wait_ms=1.0,
        cursor_timings={"wall_clock_ms": 99.9},
    )

    progress = BenchmarkProgress(total_cursors=1)
    progress.record_completion(
        123,
        latency_ms=10.0,
        total_messages=4,
        phase_timings=sample.phase_timings,
        sample=sample,
    )
    logger = BenchmarkLogBuffer()
    logger.info(
        "engine.processed",
        cursor=123,
        entity="block",
        stage="process",
        rpc_latency=4.2,
        latency_ms=4.2,
        payload=4,
    )

    console = Console(record=True, width=240, force_terminal=True, color_system="standard")
    console.print(
        render_benchmark_dashboard(
            mode="backfill",
            sink="blackhole",
            eos_enabled=False,
            window=1,
            head_cursor=123,
            progress=progress,
            logger=logger,
        )
    )

    rendered = console.export_text()
    assert "LATENCIES" in rendered
