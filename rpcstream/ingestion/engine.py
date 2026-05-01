import json
import asyncio
from contextlib import suppress
import time

from rpcstream.client.models import RpcErrorResult
from rpcstream.ingestion.dlq import (
    build_resolved_record,
    build_retry_record,
    build_unified_dlq_record,
    compute_next_retry_at,
)

from opentelemetry.trace import Status, StatusCode

from rpcstream.metrics.engine import EngineMetrics
from rpcstream.runtime.observability.context import ObservabilityContext
from rpcstream.state.checkpoint import build_checkpoint_row, build_watermark_state_row

class IngestionEngine:
    def __init__(
        self, 
        fetcher, 
        processors, 
        enricher,
        sink, 
        topics, 
        dlq_topic=None,
        dlq_topics=None,
        chain=None,
        pipeline=None,
        max_retry=0,
        concurrency=10, 
        logger=None,
        observability: ObservabilityContext | None = None,
        watermark_manager=None,
        checkpoint_reader=None,
        eos_enabled=False,
    ):
        self.fetcher = fetcher
        self.processors = processors
        self.enricher = enricher
        self.sink = sink
        self.topics = topics
        if dlq_topic is None and dlq_topics is not None:
            if isinstance(dlq_topics, dict):
                dlq_topic = next(iter(dlq_topics.values()), None)
            else:
                dlq_topic = dlq_topics

        self.dlq_topic = dlq_topic
        self.chain = chain
        self.pipeline = pipeline
        self.max_retry = max_retry
        self.concurrency = concurrency
        self.semaphore = asyncio.Semaphore(concurrency)
        self.logger = logger
        self._latest_processed_block = 0
        self._lag_lock = asyncio.Lock()
        self._active_dlq_record = None
        self.observability = observability or ObservabilityContext.disabled()
        self._tracer = self.observability.get_tracer(__name__)
        self.metrics = EngineMetrics(self.observability.get_meter("rpcstream.engine"))
        self.watermark_manager = watermark_manager
        self.checkpoint_reader = checkpoint_reader
        self.eos_enabled = eos_enabled
        self._checkpoint_tasks = set()
        self._last_phase_timings = {}
        self._last_cursor_observation = {}
        self._last_delivery_summary = {}
        self._cursor_phase_timings = {}
        self._cursor_observations = {}
        self._cursor_delivery_summaries = {}

    async def run_stream(self, cursor_source, shutdown_event: asyncio.Event | None = None):
        sink_started = False
        checkpoint_started = False
        workers = []
        await self.sink.start()
        sink_started = True
        if self.watermark_manager is not None:
            await self.watermark_manager.start()
            checkpoint_started = True

        worker_count = 1 if self.eos_enabled else self.concurrency
        queue = asyncio.Queue(maxsize=1 if self.eos_enabled else 1000)

        async def producer():
            try:
                while not self._is_shutdown_requested(shutdown_event):
                    cursor = await self._next_cursor_or_shutdown(cursor_source, shutdown_event)
                    if cursor is None:
                        break
                    if self.watermark_manager is not None:
                        await self.watermark_manager.mark_emitted(cursor)
                    await queue.put(cursor)
            finally:
                # Signal workers to drain and stop after queued blocks are processed.
                for _ in range(worker_count):
                    await queue.put(None)

        async def worker():
            while True:
                cursor = await queue.get()
                if cursor is None:
                    break
                success, delivery_futures, expected_watermark = await self._run_one(cursor)
                if self.watermark_manager is not None:
                    task = asyncio.create_task(
                        self._finalize_checkpoint(
                            cursor,
                            success,
                            delivery_futures,
                            expected_watermark=expected_watermark,
                        )
                    )
                    self._checkpoint_tasks.add(task)
                    task.add_done_callback(self._checkpoint_tasks.discard)

        try:
            workers = [
                asyncio.create_task(worker())
                for _ in range(worker_count)
            ]

            await producer()
            if self._is_shutdown_requested(shutdown_event) and self.logger:
                self.logger.warn(
                    "engine.shutdown_draining",
                    component="engine",
                    queued_blocks=queue.qsize(),
                    checkpoint_tasks=len(self._checkpoint_tasks),
                )
            await asyncio.gather(*workers)
        except asyncio.CancelledError:
            if self.logger:
                self.logger.warn(
                    "engine.shutdown_cancelled",
                    component="engine",
                )
            for task in workers:
                task.cancel()
            if workers:
                await asyncio.gather(*workers, return_exceptions=True)
        finally:
            if sink_started:
                await self.sink.close()
            if self._checkpoint_tasks:
                await asyncio.gather(*self._checkpoint_tasks, return_exceptions=True)
            if self.watermark_manager is not None and checkpoint_started:
                status = "eos" if getattr(self.pipeline, "mode", None) == "backfill" else "running"
                await self.watermark_manager.stop(status=status)

    def _is_shutdown_requested(self, shutdown_event: asyncio.Event | None) -> bool:
        return shutdown_event is not None and shutdown_event.is_set()

    async def _next_cursor_or_shutdown(self, cursor_source, shutdown_event: asyncio.Event | None):
        if shutdown_event is None:
            return await cursor_source.next_cursor()

        next_cursor_task = asyncio.create_task(cursor_source.next_cursor())
        shutdown_task = asyncio.create_task(shutdown_event.wait())
        done, pending = await asyncio.wait(
            {next_cursor_task, shutdown_task},
            return_when=asyncio.FIRST_COMPLETED,
        )

        if shutdown_task in done:
            next_cursor_task.cancel()
            with suppress(asyncio.CancelledError):
                await next_cursor_task
            return None

        shutdown_task.cancel()
        with suppress(asyncio.CancelledError):
            await shutdown_task
        return await next_cursor_task


    async def _run_one(self, cursor):
        cursor = int(cursor)
        start_total = time.time()
        start_wall_ms = int(start_total * 1000)
        phase_timings = {
            "fetch_ms": 0.0,
            "rpc_queue_total_ms": 0.0,
            "rpc_ms_total_ms": 0.0,
            "rpc_requests": 0,
            "rpc_inflight_current": 0,
            "rpc_min_ms": None,
            "rpc_max_ms": None,
            "process_ms": 0.0,
            "enrich_ms": 0.0,
            "sink_enqueue_ms": 0.0,
            "sink_delivery_ms": 0.0,
        }
        current_entity = "unknown"
        success = True
        delivery_futures = []
        expected_watermark = None
        transactional_topic_rows = []
        parsed_bundle = {}
        cursor_observation = {
            "cursor": cursor,
            "started_at_ms": start_wall_ms,
            "event_timestamp_ms": None,
            "ingest_timestamp_ms": None,
            "kafka_append_timestamp_ms": None,
            "event_to_ingest_ms": None,
            "ingest_to_kafka_ms": None,
            "event_to_kafka_ms": None,
            "delivery_wait_ms": None,
            "checkpoint_ms": None,
        }
        try:
            with self._tracer.start_as_current_span("streaming.run") as root_span:
                root_span.set_attribute("component", "engine")
                root_span.set_attribute("cursor", cursor)
                root_span.set_attribute("cursor_value", cursor)

                self.metrics.INFLIGHT.add(1)

                # 1. FETCH
                fetch_started = time.perf_counter()
                raw_data = await self.fetcher.fetch(cursor)
                phase_timings["fetch_ms"] = (time.perf_counter() - fetch_started) * 1000

                for entity, processor in self.processors.items():
                    current_entity = entity
                    if isinstance(raw_data[entity], RpcErrorResult):
                        error_msg = raw_data[entity].error
                        error_details = raw_data[entity].details.copy()
                        self.metrics.ERROR_COUNTER.add(1, {"stage": "rpc"})

                        if self.logger:
                            self.logger.warn(
                                "engine.rpc_failed",
                                component="engine",
                                entity=entity,
                                cursor=cursor,
                                error=error_msg,
                                expected=raw_data[entity].expected,
                                **{
                                    key: value
                                    for key, value in error_details.items()
                                    if key != "block"
                                },
                            )
                        await self._send_dlq(
                            entity=entity,
                            cursor=cursor,
                            stage="rpc",
                            error_type="RpcError",
                            error_message=error_msg,
                            payload=None,
                            context={
                                "request": raw_data[entity].meta.extra,
                                "rpc_error": error_details,
                                "expected": raw_data[entity].expected,
                            },
                        )
                        success = False
                        return False, delivery_futures

                    try:
                        value, meta = raw_data[entity]
                        process_started = time.perf_counter()
                        processed_data = processor.process(cursor, value)
                        phase_timings["process_ms"] += (time.perf_counter() - process_started) * 1000
                        for processed_entity, rows in processed_data.items():
                            parsed_bundle.setdefault(processed_entity, []).extend(rows)

                        head_cursor, head_lag, ingestion_lag = await self._compute_lag(cursor)
                        if head_lag is not None:
                            self.metrics.CHAIN_LAG.record(head_lag)
                        if ingestion_lag is not None:
                            self.metrics.INGESTION_LAG.record(ingestion_lag)

                        latency = meta.extra.get("latency_ms", 0)
                        queue_wait = meta.extra.get("queue_wait_ms", 0)
                        inflight = meta.extra.get("inflight", 0)
                        phase_timings["rpc_requests"] += 1
                        phase_timings["rpc_ms_total_ms"] += float(latency)
                        phase_timings["rpc_queue_total_ms"] += float(queue_wait)
                        phase_timings["rpc_inflight_current"] = int(inflight)
                        phase_timings["rpc_min_ms"] = (
                            float(latency)
                            if phase_timings["rpc_min_ms"] is None
                            else min(float(latency), phase_timings["rpc_min_ms"])
                        )
                        phase_timings["rpc_max_ms"] = (
                            float(latency)
                            if phase_timings["rpc_max_ms"] is None
                            else max(float(latency), phase_timings["rpc_max_ms"])
                        )

                        self.metrics.BLOCK_COUNTER.add(1, {"entity": entity})
                        self.metrics.BLOCK_LATENCY.record(latency, {"entity": entity})
                        self.metrics.QUEUE_WAIT.record(queue_wait, {"entity": entity})
                        emitted_rows = sum(len(rows) for rows in processed_data.values())
                        if self.logger:
                            self.logger.info(
                                "engine.processed",
                                component="engine",
                                cursor=cursor,
                                entity=entity,
                                latency_ms=latency,
                                payload=emitted_rows,
                                ingestion_lag=ingestion_lag,
                            )
                    except Exception as e:
                        await self._send_dlq(
                            entity=entity,
                            cursor=cursor,
                            stage="processor",
                            error_type=type(e).__name__,
                            error_message=str(e),
                            payload=value,
                            context={
                                "processor": processor.__class__.__name__,
                                "meta": meta.extra,
                            },
                        )
                        success = False

                if success:
                    enrich_started = time.perf_counter()
                    final_bundle = self.enricher.enrich(parsed_bundle) if self.enricher else parsed_bundle
                    phase_timings["enrich_ms"] += (time.perf_counter() - enrich_started) * 1000
                    cursor_observation["event_timestamp_ms"] = self._extract_event_timestamp_ms(
                        final_bundle
                    )
                    for entity, topic in self.topics.items():
                        rows = final_bundle.get(entity, [])
                        self.metrics.ROW_COUNTER.add(len(rows), {"entity": entity})
                        if not rows:
                            continue
                        sink_started = time.perf_counter()
                        if self.eos_enabled:
                            transactional_topic_rows.append((topic, rows))
                        else:
                            delivery_future = await self.sink.send(
                                topic,
                                rows,
                                wait_delivery=self.watermark_manager is not None,
                            )
                            if delivery_future is not None:
                                delivery_futures.append(delivery_future)
                        phase_timings["sink_enqueue_ms"] += (time.perf_counter() - sink_started) * 1000
        except Exception as e:
            with self._tracer.start_as_current_span("engine.error") as error_span:
                error_span.set_status(Status(StatusCode.ERROR))
                error_span.set_attribute("error.message", str(e))
                error_span.set_attribute("entity", current_entity)
                error_span.set_attribute("cursor_value", cursor)

            error_msg = repr(e)
            self.metrics.ERROR_COUNTER.add(1, {"stage": "processor"})

            if self.logger:
                self.logger.error(
                    "engine.processor_error",
                    component="engine",
                    entity=current_entity,
                    cursor=cursor,
                    error=error_msg
                )

            await self._send_dlq(
                entity=current_entity,
                cursor=cursor,
                stage="processor",
                error_type=type(e).__name__,
                error_message=str(e),
                payload=None,
                context={},
            )
            success = False

        finally:
            rpc_requests = max(int(phase_timings.get("rpc_requests", 0)), 0)
            if rpc_requests > 0:
                phase_timings["rpc_ms"] = phase_timings["rpc_ms_total_ms"] / rpc_requests
                phase_timings["rpc_queue_ms"] = phase_timings["rpc_queue_total_ms"] / rpc_requests
            else:
                phase_timings["rpc_ms"] = 0.0
                phase_timings["rpc_queue_ms"] = 0.0
                phase_timings["rpc_min_ms"] = 0.0
                phase_timings["rpc_max_ms"] = 0.0
            phase_timings["sink_ms"] = (
                phase_timings["sink_enqueue_ms"] + phase_timings["sink_delivery_ms"]
            )
            self._last_phase_timings = dict(phase_timings)
            self._cursor_phase_timings[cursor] = dict(phase_timings)
            self._last_cursor_observation = dict(cursor_observation)
            self._cursor_observations[cursor] = dict(cursor_observation)
            self.metrics.INFLIGHT.add(-1)
            total_ms = (time.time() - start_total) * 1000
            self.metrics.TOTAL_TIME.record(total_ms, {"entity": current_entity})

        if success and self.eos_enabled:
            should_persist_cursor_state = False
            if self.watermark_manager is not None:
                should_persist_cursor_state = await self.watermark_manager.requires_cursor_state(
                    cursor
                )
                expected_watermark = await self.watermark_manager.preview_completed(cursor)
                if should_persist_cursor_state:
                    transactional_topic_rows.append(
                        (
                            self.watermark_manager.state_topic,
                            [
                                build_watermark_state_row(
                                    self.watermark_manager.identity,
                                    cursor,
                                    status="completed",
                                )
                            ],
                        )
                    )
            if expected_watermark is not None and self.watermark_manager is not None:
                transactional_topic_rows.append(
                    (
                        self.watermark_manager.topic,
                        [
                            build_checkpoint_row(
                                self.watermark_manager.identity,
                                expected_watermark,
                                status="running",
                            )
                        ],
                    )
                )
            delivery_future = await self.sink.send_transaction(transactional_topic_rows)
            if delivery_future is not None:
                delivery_futures.append(delivery_future)
        return success, delivery_futures, expected_watermark

    async def _finalize_checkpoint(
        self,
        cursor,
        success,
        delivery_futures,
        *,
        expected_watermark=None,
    ):
        if not success:
            await self._record_failed_watermark_state(cursor)
            return
        try:
            delivery_results = []
            if delivery_futures:
                delivery_results = await asyncio.gather(*delivery_futures)
                delivery_summary = self._aggregate_delivery_summaries(delivery_results)
                if delivery_summary:
                    self._last_delivery_summary = delivery_summary
                    self._cursor_delivery_summaries[cursor] = delivery_summary
                    self._last_cursor_observation.update(delivery_summary)
                    self._cursor_observations.setdefault(cursor, {}).update(delivery_summary)
                    cursor_phase_timings = self._cursor_phase_timings.setdefault(
                        cursor,
                        dict(self._last_phase_timings),
                    )
                    if delivery_summary.get("delivery_wait_ms") is not None:
                        cursor_phase_timings["sink_delivery_ms"] = delivery_summary["delivery_wait_ms"]
                        cursor_phase_timings["sink_ms"] = (
                            cursor_phase_timings.get("sink_enqueue_ms", 0.0)
                            + cursor_phase_timings.get("sink_delivery_ms", 0.0)
                        )
                        self._last_phase_timings["sink_delivery_ms"] = delivery_summary["delivery_wait_ms"]
                        self._last_phase_timings["sink_ms"] = (
                            self._last_phase_timings.get("sink_enqueue_ms", 0.0)
                            + self._last_phase_timings.get("sink_delivery_ms", 0.0)
                        )
                    self._last_phase_timings = dict(cursor_phase_timings)
            should_persist_cursor_state = False
            if self.watermark_manager is not None:
                should_persist_cursor_state = await self.watermark_manager.requires_cursor_state(
                    cursor
                )
            if not self.eos_enabled and should_persist_cursor_state:
                checkpoint_future = await self.sink.send(
                    self.watermark_manager.state_topic,
                    [
                        build_watermark_state_row(
                            self.watermark_manager.identity,
                            cursor,
                            status="completed",
                        )
                    ],
                    wait_delivery=True,
                )
                if checkpoint_future is not None:
                    checkpoint_result = await checkpoint_future
                    if isinstance(checkpoint_result, dict):
                        self._last_cursor_observation["checkpoint_delivery_summary"] = checkpoint_result
                        self._last_cursor_observation["checkpoint_delivery_wait_ms"] = checkpoint_result.get(
                            "delivery_wait_ms"
                        )
                        self._cursor_observations.setdefault(cursor, {})["checkpoint_delivery_summary"] = checkpoint_result
                        self._cursor_observations.setdefault(cursor, {})["checkpoint_delivery_wait_ms"] = checkpoint_result.get(
                            "delivery_wait_ms"
                        )
            advanced_watermark = await self.watermark_manager.mark_completed(cursor)
            if (
                self.eos_enabled
                and expected_watermark is not None
                and advanced_watermark != expected_watermark
                and self.logger is not None
            ):
                self.logger.warn(
                    "watermark.advance_mismatch",
                    component="checkpoint",
                    cursor=cursor,
                    expected=expected_watermark,
                    actual=advanced_watermark,
                )
        except Exception as exc:
            await self._record_failed_watermark_state(cursor, error=str(exc))
            return

    async def _record_failed_watermark_state(self, cursor, error: str | None = None):
        if self.watermark_manager is None:
            return
        await self.watermark_manager.mark_failed(cursor, error=error)
        row = build_watermark_state_row(
            self.watermark_manager.identity,
            cursor,
            status="failed",
            error=error,
        )
        if self.eos_enabled:
            delivery_future = await self.sink.send_transaction([(self.watermark_manager.state_topic, [row])])
            if delivery_future is not None:
                await delivery_future
            return
        checkpoint_future = await self.sink.send(
            self.watermark_manager.state_topic,
            [row],
            wait_delivery=True,
        )
        if checkpoint_future is not None:
            await checkpoint_future



    async def _send_dlq(
        self,
        entity,
        cursor,
        stage,
        error_type,
        error_message,
        payload=None,
        context=None,
    ):
        topic = self.dlq_topic
        self.metrics.DLQ_COUNTER.add(1, {"entity": entity, "stage": stage})
        
        if not topic:
            if self.logger:
                self.logger.warn(
                    "engine.dlq_missing_topic",
                    component="engine",
                    entity=entity,
                    cursor=cursor,
                )
            return

        if self._active_dlq_record is not None:
            record = build_retry_record(
                self._active_dlq_record,
                error_type=error_type,
                error_message=error_message,
                payload=payload,
                context=context,
            )
        else:
            record = build_unified_dlq_record(
                chain=getattr(self.chain, "type", "unknown"),
                network=getattr(self.chain, "network_label", "unknown"),
                pipeline=getattr(self.pipeline, "name", "unknown"),
                entity=entity,
                cursor=cursor,
                stage=stage,
                error_type=error_type,
                error_message=error_message,
                payload=payload,
                context=context,
                retry_count=0,
                max_retry=self.max_retry,
                status="pending",
                next_retry_at=compute_next_retry_at(retry_count=1),
            )

        if self.eos_enabled:
            delivery_future = await self.sink.send_transaction([(topic, [record])])
            if delivery_future is not None:
                await delivery_future
        else:
            delivery_future = await self.sink.send(topic, [record])
            if delivery_future is not None:
                await delivery_future

        if self.logger:
            self.logger.warn(
                "engine.dlq_sent",
                component="engine",
                topic=topic,
                entity=entity,
                cursor=cursor,
                stage=stage,
                error_type=error_type,
                error=error_message,
                status=record["status"],
                retry_count=record["retry_count"],
            )

    async def retry_dlq_record(self, record: dict) -> bool:
        previous = self._active_dlq_record
        self._active_dlq_record = record
        try:
            success, delivery_futures, expected_watermark = await self._run_one(record.get("cursor"))
            if self.watermark_manager is not None:
                await self._finalize_checkpoint(
                    record.get("cursor"),
                    success,
                    delivery_futures,
                    expected_watermark=expected_watermark,
                )
            return success
        finally:
            self._active_dlq_record = previous

    async def mark_dlq_resolved(self, record: dict) -> None:
        if not self.dlq_topic:
            return
        resolved_record = build_resolved_record(record)
        if self.eos_enabled:
            delivery_future = await self.sink.send_transaction([(self.dlq_topic, [resolved_record])])
            if delivery_future is not None:
                await delivery_future
            return
        delivery_future = await self.sink.send(self.dlq_topic, [resolved_record])
        if delivery_future is not None:
            await delivery_future

    def _aggregate_delivery_summaries(self, results):
        summaries = [result for result in results if isinstance(result, dict)]
        if not summaries:
            return {}

        event_timestamps = [
            item["event_timestamp_ms"]
            for item in summaries
            if item.get("event_timestamp_ms") is not None
        ]
        ingest_timestamps = [
            item["ingest_timestamp_ms"]
            for item in summaries
            if item.get("ingest_timestamp_ms") is not None
        ]
        kafka_timestamps = [
            item["kafka_append_timestamp_ms"]
            for item in summaries
            if item.get("kafka_append_timestamp_ms") is not None
        ]
        delivery_waits = [
            item["delivery_wait_ms"]
            for item in summaries
            if item.get("delivery_wait_ms") is not None
        ]

        event_timestamp_ms = min(event_timestamps) if event_timestamps else None
        ingest_timestamp_ms = min(ingest_timestamps) if ingest_timestamps else None
        kafka_append_timestamp_ms = max(kafka_timestamps) if kafka_timestamps else None
        delivery_wait_ms = max(delivery_waits) if delivery_waits else None

        return {
            "event_timestamp_ms": event_timestamp_ms,
            "ingest_timestamp_ms": ingest_timestamp_ms,
            "kafka_append_timestamp_ms": kafka_append_timestamp_ms,
            "event_to_ingest_ms": (
                round(ingest_timestamp_ms - event_timestamp_ms, 2)
                if event_timestamp_ms is not None and ingest_timestamp_ms is not None
                else None
            ),
            "ingest_to_kafka_ms": (
                round(kafka_append_timestamp_ms - ingest_timestamp_ms, 2)
                if kafka_append_timestamp_ms is not None and ingest_timestamp_ms is not None
                else None
            ),
            "event_to_kafka_ms": (
                round(kafka_append_timestamp_ms - event_timestamp_ms, 2)
                if kafka_append_timestamp_ms is not None and event_timestamp_ms is not None
                else None
            ),
            "delivery_wait_ms": delivery_wait_ms,
            "delivery_message_count": sum(int(item.get("message_count", 0)) for item in summaries),
        }

    async def _update_ingestion_lag(self, cursor, head_cursor):
        async with self._lag_lock:
            if cursor > self._latest_processed_block:
                self._latest_processed_block = cursor

            ingestion_lag = None
            if head_cursor is not None:
                ingestion_lag = head_cursor - self._latest_processed_block

            return ingestion_lag

    def _extract_event_timestamp_ms(self, bundle: dict) -> int | None:
        timestamps: list[int] = []
        for rows in bundle.values():
            if not isinstance(rows, list):
                continue
            for row in rows:
                if not isinstance(row, dict):
                    continue
                ts = row.get("block_timestamp")
                if ts is None and row.get("type") == "block":
                    ts = row.get("timestamp")
                if ts is None:
                    continue
                try:
                    timestamps.append(int(ts) * 1000)
                except Exception:
                    continue

        if not timestamps:
            return None
        return min(timestamps)
        
    
    async def _compute_lag(self, cursor):
        head_cursor = None
        head_lag = None
        ingestion_lag = None
        pipeline_mode = getattr(self.pipeline, "mode", None)

        if pipeline_mode == "backfill":
            end_cursor = getattr(self.pipeline, "end_cursor", None)
            if end_cursor is not None:
                ingestion_lag = max(int(end_cursor) - int(cursor), 0)
                if self.watermark_manager is not None and self.watermark_manager.cursor is not None:
                    self.watermark_manager.update_commit_delay(
                        max(int(end_cursor) - int(self.watermark_manager.cursor), 0)
                    )
                elif self.watermark_manager is not None:
                    self.watermark_manager.update_commit_delay(None)
            return head_cursor, head_lag, ingestion_lag

        tracker = getattr(self.fetcher, "tracker", None)

        if tracker:
            head_cursor = tracker.get_head_cursor() if hasattr(tracker, "get_head_cursor") else tracker.get_latest()

            if head_cursor is not None:
                # point-in-time lag
                head_lag = head_cursor - cursor

                # true pipeline lag (protected update)
                ingestion_lag = await self._update_ingestion_lag(
                    cursor,
                    head_cursor
                )
                if self.watermark_manager is not None and self.watermark_manager.cursor is not None:
                    self.watermark_manager.update_commit_delay(
                        max(int(head_cursor) - int(self.watermark_manager.cursor), 0)
                    )
                elif self.watermark_manager is not None:
                    self.watermark_manager.update_commit_delay(None)
        elif self.watermark_manager is not None:
            self.watermark_manager.update_commit_delay(None)

        return head_cursor, head_lag, ingestion_lag
