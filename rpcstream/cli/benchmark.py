from __future__ import annotations

import asyncio
import sys
import time
from collections import defaultdict
from contextlib import suppress
from pathlib import Path

import typer
from confluent_kafka import Producer
from rich.console import Console
from rich.live import Live

from rpcstream.adapters import build_chain_adapter
from rpcstream.client.jsonrpc import JsonRpcClient
from rpcstream.config.builder import build_erpc_endpoint
from rpcstream.config.loader import load_pipeline_config
from rpcstream.config.overrides import apply_runtime_overrides
from rpcstream.config.resolver import resolve
from rpcstream.dashboard import (
    BenchmarkLogBuffer,
    BenchmarkProgress,
    BenchmarkSample,
    BenchmarkSummary,
    render_benchmark_dashboard,
    wait_for_exit_keypress,
    write_benchmark_output_file,
)
from rpcstream.ingestion.engine import IngestionEngine
from rpcstream.planner.cursor_source import build_cursor_source
from rpcstream.runtime.observability.provider import build_observability
from rpcstream.scheduler.adaptive import AdaptiveRpcScheduler
from rpcstream.sinks.blackhole import BlackholeSink
from rpcstream.sinks.kafka.producer import KafkaWriter
from rpcstream.state.checkpoint import (
    WatermarkManager,
    build_checkpoint_identity,
)


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


class NoopWatermarkManager:
    """Minimal watermark manager used by benchmark runs."""

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
    """Sink wrapper that tracks how many rows and batches benchmark writes."""

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

    async def send_checkpoint(self, topic, row, wait_delivery=True):
        return await self.sink.send_checkpoint(topic, row, wait_delivery=wait_delivery)


def _build_benchmark_sample(
    *,
    cursor: int,
    cursor_started_at: float,
    checkpoint_ms: float,
    engine,
    head_observed_at_ms: int | None = None,
    cursor_emitted_at_ms: int | None = None,
    tracker_poll_latency_ms: float | None = None,
    watermark_manager=None,
    message_count: int,
) -> BenchmarkSample:
    phase_timings = dict(getattr(engine, "_cursor_phase_timings", {}).get(cursor, {}))
    cursor_observation = dict(getattr(engine, "_cursor_observations", {}).get(cursor, {}))
    delivery_summary = dict(getattr(engine, "_cursor_delivery_summaries", {}).get(cursor, {}))

    event_timestamp_ms = (
        delivery_summary.get("event_timestamp_ms")
        if delivery_summary.get("event_timestamp_ms") is not None
        else cursor_observation.get("event_timestamp_ms")
    )
    ingest_timestamp_ms = delivery_summary.get("ingest_timestamp_ms")
    kafka_append_timestamp_ms = delivery_summary.get("kafka_append_timestamp_ms")
    event_to_ingest_ms = delivery_summary.get("event_to_ingest_ms")
    ingest_to_kafka_ms = delivery_summary.get("ingest_to_kafka_ms")
    event_to_kafka_ms = delivery_summary.get("event_to_kafka_ms")
    delivery_wait_ms = delivery_summary.get("delivery_wait_ms")
    checkpoint_delivery_wait_ms = cursor_observation.get("checkpoint_delivery_wait_ms")
    if checkpoint_delivery_wait_ms is None and watermark_manager is not None:
        checkpoint_delivery_wait_ms = getattr(watermark_manager, "last_delivery_wait_ms", None)
    if checkpoint_delivery_wait_ms is None:
        checkpoint_delivery_wait_ms = delivery_summary.get("delivery_wait_ms")
    head_observed_to_emit_ms = None
    if head_observed_at_ms is not None and cursor_emitted_at_ms is not None:
        head_observed_to_emit_ms = round(float(cursor_emitted_at_ms - head_observed_at_ms), 2)
    head_observed_lag_ms = None
    if head_observed_at_ms is not None and event_timestamp_ms is not None:
        head_observed_lag_ms = round(float(head_observed_at_ms - event_timestamp_ms), 2)
    wall_clock_ms = (time.perf_counter() - cursor_started_at) * 1000
    latency_ms = event_to_kafka_ms if event_to_kafka_ms is not None else wall_clock_ms

    if event_timestamp_ms is not None:
        cursor_observation["event_timestamp_ms"] = event_timestamp_ms
    if ingest_timestamp_ms is not None:
        cursor_observation["ingest_timestamp_ms"] = ingest_timestamp_ms
    if kafka_append_timestamp_ms is not None:
        cursor_observation["kafka_append_timestamp_ms"] = kafka_append_timestamp_ms
    if checkpoint_delivery_wait_ms is not None:
        cursor_observation["checkpoint_delivery_wait_ms"] = checkpoint_delivery_wait_ms
    cursor_observation["checkpoint_ms"] = checkpoint_ms
    cursor_observation["wall_clock_ms"] = wall_clock_ms

    sample = BenchmarkSample(
        cursor=cursor,
        latency_ms=latency_ms,
        message_count=max(message_count, 0),
        completed_at=time.time() * 1000,
        phase_timings=phase_timings,
        event_timestamp_ms=event_timestamp_ms,
        ingest_timestamp_ms=ingest_timestamp_ms,
        kafka_append_timestamp_ms=kafka_append_timestamp_ms,
        event_to_ingest_ms=event_to_ingest_ms,
        ingest_to_kafka_ms=ingest_to_kafka_ms,
        event_to_kafka_ms=event_to_kafka_ms,
        delivery_wait_ms=delivery_wait_ms,
        head_observed_at_ms=head_observed_at_ms,
        cursor_emitted_at_ms=cursor_emitted_at_ms,
        head_observed_to_emit_ms=head_observed_to_emit_ms,
        head_observed_lag_ms=head_observed_lag_ms,
        tracker_poll_latency_ms=tracker_poll_latency_ms,
        cursor_timings=cursor_observation,
    )
    return sample


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
        benchmark_mode = mode.strip().lower()
        if benchmark_mode not in {"backfill", "realtime"}:
            raise ValueError("--mode must be either 'backfill' or 'realtime'")

        if benchmark_mode == "backfill":
            start_cursor = head_cursor - window + 1
            if start_cursor < 0:
                raise ValueError(
                    f"window={window} is larger than the current chainhead {head_cursor}"
                )
            benchmark_from_value: str | int = start_cursor
            benchmark_to_value: int | None = head_cursor
            summary_start_cursor = start_cursor
        else:
            benchmark_from_value = "chainhead"
            benchmark_to_value = None
            summary_start_cursor = head_cursor

        benchmark_config = apply_runtime_overrides(
            raw_config,
            mode=benchmark_mode,
            from_value=benchmark_from_value,
            to_value=benchmark_to_value,
            entities=_parse_entities(entity),
            eos_enabled=(benchmark_mode == "realtime"),
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
        if runtime.pipeline.mode == "realtime" and sink.lower().strip() == "kafka":
            checkpoint_identity = build_checkpoint_identity(runtime)
            state_reader = None
            state_records = {}
            watermark_manager = WatermarkManager(
                sink=sink_obj,
                topic=runtime.checkpoint.topic,
                state_topic=runtime.checkpoint.watermark_state_topic,
                identity=checkpoint_identity,
                initial_cursor=None,
                state_records=state_records,
                state_reader=state_reader,
                flush_interval_ms=runtime.checkpoint.flush_interval_ms,
                commit_batch_size=runtime.checkpoint.commit_batch_size,
                flush_on_advance=False,
                logger=logger,
                meter=observability.get_meter("rpcstream.watermark"),
            )
        tracker = None
        if runtime.pipeline.mode == "realtime":
            tracker = adapter.build_tracker(
                client=run_client,
                poll_interval=runtime.tracker.poll_interval,
                logger=logger,
            )
            await tracker.start()
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
                tracker=tracker,
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

        cursor_source = build_cursor_source(runtime, tracker, observability=observability, resume_cursor=None)
        samples: list[BenchmarkSample] = []
        progress = BenchmarkProgress(total_cursors=window)
        progress_lock = asyncio.Lock()
        dashboard_stop = asyncio.Event()
        dashboard_console = Console(file=sys.stderr, force_terminal=True, color_system="auto")
        scheduler = engine.fetcher.scheduler

        async def refresh_dashboard(live: Live):
            while not dashboard_stop.is_set():
                live.update(
                        render_benchmark_dashboard(
                            mode=benchmark_mode,
                            sink=sink.lower().strip(),
                            eos_enabled=runtime.kafka.eos_enabled,
                            window=window,
                            head_cursor=head_cursor,
                            progress=progress,
                            logger=logger,
                            scheduler=scheduler,
                        )
                    )
                live.refresh()
                progress.mark_refreshed()
                await asyncio.sleep(1.0)

        total_started = time.perf_counter()

        await sink_obj.start()
        with Live(
            render_benchmark_dashboard(
                mode=benchmark_mode,
                sink=sink.lower().strip(),
                eos_enabled=runtime.kafka.eos_enabled,
                window=window,
                head_cursor=head_cursor,
                progress=progress,
                logger=logger,
                scheduler=scheduler,
            ),
            console=dashboard_console,
            screen=True,
            auto_refresh=False,
            transient=False,
        ) as live:
            dashboard_task = asyncio.create_task(refresh_dashboard(live))
            try:
                worker_count = 1 if runtime.kafka.eos_enabled else max(1, runtime.engine.concurrency)
                queue_size = max(1, max(runtime.scheduler.max_inflight, runtime.engine.concurrency) * 2)
                queue: asyncio.Queue[tuple[int, float, int | None, int | None, float | None] | None] = asyncio.Queue(maxsize=queue_size)

                async def producer():
                    try:
                        emitted = 0
                        while emitted < window:
                            cursor = await cursor_source.next_cursor()
                            if cursor is None:
                                break
                            head_observed_at_ms = getattr(cursor_source, "last_head_observed_at_ms", None)
                            cursor_emitted_at_ms = getattr(cursor_source, "last_cursor_emitted_at_ms", None)
                            tracker_poll_latency_ms = None
                            if tracker is not None:
                                poll_started = getattr(tracker, "get_last_poll_started_at_ms", lambda: None)()
                                poll_completed = getattr(tracker, "get_last_poll_completed_at_ms", lambda: None)()
                                if poll_started is not None and poll_completed is not None:
                                    tracker_poll_latency_ms = round(float(poll_completed - poll_started), 2)
                            await queue.put(
                                (
                                    int(cursor),
                                    time.perf_counter(),
                                    head_observed_at_ms,
                                    cursor_emitted_at_ms,
                                    tracker_poll_latency_ms,
                                )
                            )
                            emitted += 1
                    finally:
                        for _ in range(worker_count):
                            await queue.put(None)

                async def worker():
                    while True:
                        item = await queue.get()
                        if item is None:
                            break
                        cursor, cursor_started, head_observed_at_ms, cursor_emitted_at_ms, tracker_poll_latency_ms = item
                        if isinstance(watermark_manager, WatermarkManager):
                            await watermark_manager.mark_emitted(cursor)
                        success, delivery_futures, expected_watermark = await engine._run_one(cursor)
                        checkpoint_started = time.perf_counter()
                        await engine._finalize_checkpoint(
                            cursor,
                            success,
                            delivery_futures,
                            expected_watermark=expected_watermark,
                        )
                        if isinstance(watermark_manager, WatermarkManager) and not runtime.kafka.eos_enabled:
                            await watermark_manager.flush(force=True)
                        checkpoint_ms = (time.perf_counter() - checkpoint_started) * 1000
                        latency_ms = (time.perf_counter() - cursor_started) * 1000
                        sample = _build_benchmark_sample(
                            cursor=cursor,
                            cursor_started_at=cursor_started,
                            checkpoint_ms=checkpoint_ms,
                            engine=engine,
                            head_observed_at_ms=head_observed_at_ms,
                            cursor_emitted_at_ms=cursor_emitted_at_ms,
                            tracker_poll_latency_ms=tracker_poll_latency_ms,
                            watermark_manager=watermark_manager,
                            message_count=0,
                        )
                        phase_timings = dict(sample.phase_timings)
                        phase_timings["checkpoint_ms"] = checkpoint_ms
                        phase_timings["e2e_ms"] = sample.event_to_kafka_ms if sample.event_to_kafka_ms is not None else latency_ms
                        progress_latency_ms = phase_timings["e2e_ms"]
                        async with progress_lock:
                            delta_messages = progress.record_completion(
                                cursor,
                                latency_ms=progress_latency_ms,
                                total_messages=sink_obj.message_count,
                                phase_timings=phase_timings,
                            )
                        sample.message_count = delta_messages
                        sample.phase_timings = phase_timings
                        async with progress_lock:
                            progress.recent_samples.append(sample)
                            samples.append(sample)

                workers = [asyncio.create_task(worker()) for _ in range(worker_count)]
                await producer()
                await asyncio.gather(*workers)
            finally:
                dashboard_stop.set()
                await dashboard_task
                live.update(
                    render_benchmark_dashboard(
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
                    await asyncio.to_thread(wait_for_exit_keypress)
        await sink_obj.close()
        if tracker is not None:
            await tracker.stop()
            tracker = None
        total_elapsed_sec = time.perf_counter() - total_started
        total_messages = sink_obj.message_count
        total_cursors = len(samples)
        summary_end_cursor = samples[-1].cursor if samples else head_cursor
        summary = BenchmarkSummary(
            mode=benchmark_mode,
            chain_name=runtime.chain.name,
            network=runtime.chain.network,
            sink=sink.lower().strip(),
            eos_enabled=runtime.kafka.eos_enabled,
            start_cursor=summary_start_cursor,
            end_cursor=summary_end_cursor,
            total_cursors=total_cursors,
            total_messages=total_messages,
            total_elapsed_sec=total_elapsed_sec,
            samples=samples,
        )
        if output_file:
            write_benchmark_output_file(summary, output_file)
        return summary
    finally:
        if "tracker" in locals() and tracker is not None:
            with suppress(Exception):
                await tracker.stop()
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
    window: int = typer.Option(
        1000,
        "--window",
        help="Number of cursors to process. Backfill uses a bounded range ending at chainhead; realtime collects the next N live cursors.",
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
        "backfill",
        "--mode",
        help="Benchmark mode: backfill for bounded replay or realtime for live chain ingestion.",
    ),
    output_file: str | None = typer.Option(
        None,
        "--output-file",
        help="Optional JSON output file. If set without a suffix, .json is appended.",
    ),
) -> None:
    """Benchmark ingestion latency and throughput and show a live dashboard."""
    config_path = config_path or _default_config_path()
    try:
        asyncio.run(
            _run_benchmark_async(
                config_path=config_path,
                sink=sink,
                mode=mode,
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


_write_benchmark_output_file = write_benchmark_output_file


__all__ = [
    "benchmark",
    "_default_config_path",
    "_run_benchmark_async",
    "_write_benchmark_output_file",
    "NoopWatermarkManager",
    "CountingSink",
]
