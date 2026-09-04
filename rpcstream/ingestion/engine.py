import json
import asyncio
import os
from contextlib import suppress
import time

from rpcstream.client.models import RpcErrorResult
from rpcstream.ingestion.dlq import (
    build_resolved_record,
    build_retry_record,
    build_unified_dlq_record,
    compute_next_retry_at,
)
from rpcstream.config.profiles.store import get_chain_profile

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
        concurrency=0,
        max_inflight: int = 1,
        sink_failure_timeout_sec: float = 10.0,
        sink_cooldown_sec: float = 15.0,
        sink_inflight_cursors: int = 2,
        logger=None,
        observability: ObservabilityContext | None = None,
        decoder=None,
        watermark_manager=None,
        checkpoint_reader=None,
        eos_enabled=False,
        upstream_not_ready_max_attempts: int = 3,
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
        # Upper bound for the adaptive worker pool. Only used when
        # concurrency == 0 (adaptive mode) — we spawn that many workers at
        # startup and let cooperative shrink reduce the active count when the
        # scheduler's current_limit drops (CB trip, latency inflation, ...).
        self.max_inflight = max(1, int(max_inflight))
        # Track the live worker count for the OTel observable gauge.
        # Populated when run_stream spawns the workers.
        self._active_worker_count = 0
        self._worker_exit_flags: list[asyncio.Event] = []
        # Wall-clock time of the last producer()/worker() loop iteration.
        # Live incident: rpcstream-log's event loop went genuinely idle for
        # 5+ minutes with no crash, no error, no ingestion_paused log (which
        # would have fired if this were the known sink-cooldown pause) --
        # two py-spy dumps minutes apart were identical, and the exact
        # stuck coroutine couldn't be pinpointed. These timestamps, exported
        # as gauges (see EngineMetrics), turn "time() - heartbeat" into a
        # dashboardable staleness signal so a repeat doesn't need a debug
        # pod + py-spy to even notice, let alone diagnose.
        self._producer_heartbeat_ts = 0.0
        self._worker_heartbeat_ts = 0.0
        self.sink_failure_timeout_sec = sink_failure_timeout_sec
        self.sink_cooldown_sec = sink_cooldown_sec
        # Max cursors concurrently inside the sink pipeline (sent to the sink
        # but not yet delivery-confirmed). This is the real backpressure
        # boundary between the engine and Kafka: each cursor enqueues its whole
        # row-set as batches, so unbounded in-flight cursors fill the sink queue
        # and make every delivery future wait behind a huge backlog -- timing
        # out at sink_failure_timeout_sec even though the sink is healthy and
        # draining. Limiting in-flight cursors bounds sink-queue depth, so a
        # slow sink backpressures at the cursor boundary and a busy-but-alive
        # sink is never mistaken for a failed one.
        self.sink_inflight_cursors = max(1, int(sink_inflight_cursors))
        # When the sink (Kafka) is unavailable, pulling new cursors is paused
        # and checkpoint waits are bounded so we don't accumulate unbounded
        # failed work / memory. See _should_pause_ingestion / _finalize_checkpoint.
        self._sink_unhealthy_until = None
        # Throttle for the "still paused" diagnostic log (see
        # _should_pause_ingestion) so a stall is visible in plain
        # `docker compose logs` without spamming every 0.2s poll.
        self._last_pause_log_ts = 0.0
        self.pause_log_interval_sec = 10.0
        self.semaphore = asyncio.Semaphore(concurrency)
        self.logger = logger
        self._latest_processed_block = 0
        self._lag_lock = asyncio.Lock()
        self._active_dlq_record = None
        self.observability = observability or ObservabilityContext.disabled()
        self._tracer = self.observability.get_tracer(__name__)
        self.metrics = EngineMetrics(
            self.observability.get_meter("rpcstream.engine"),
            engine=self,
        )
        self.decoder = decoder
        self.watermark_manager = watermark_manager
        self.checkpoint_reader = checkpoint_reader
        self.eos_enabled = eos_enabled
        self.upstream_not_ready_max_attempts = max(1, int(upstream_not_ready_max_attempts))
        self._checkpoint_tasks = set()
        self._last_phase_timings = {}
        self._last_cursor_observation = {}
        self._last_delivery_summary = {}
        self._cursor_phase_timings = {}
        self._cursor_observations = {}
        self._cursor_delivery_summaries = {}
        self._progress_last_ts = 0.0

    async def run_stream(self, cursor_source, shutdown_event: asyncio.Event | None = None):
        sink_started = False
        checkpoint_started = False
        workers = []
        await self.sink.start()
        sink_started = True
        if self.watermark_manager is not None:
            await self.watermark_manager.start()
            checkpoint_started = True

        queue = asyncio.Queue(maxsize=1 if self.eos_enabled else 1000)
        # Cap how many cursors may be simultaneously in the sink pipeline
        # (fully enqueued, delivery not yet confirmed). See __init__ comment.
        # EOS mode is strict-serial anyway; use the configured limit otherwise.
        sink_gate = asyncio.Semaphore(
            1 if self.eos_enabled else self.sink_inflight_cursors
        )
        # Worker pool sizing:
        #   - eos_enabled: 1 worker, single permit, strict serial.
        #   - concurrency == 0 (adaptive): spawn max_inflight workers, register
        #     a scheduler callback that shrinks the active set when the
        #     scheduler's current_limit drops. Grow is free (workers already
        #     exist); constraint `active >= current_limit` is upheld because
        #     shrink only marks excess workers for exit, never the ones still
        #     needed.
        #   - concurrency > 0 (fixed): spawn N workers, no adaptive behaviour.
        if self.eos_enabled:
            worker_pool_size = 1
        elif self.concurrency == 0:
            worker_pool_size = self.max_inflight
        else:
            worker_pool_size = self.concurrency
        is_adaptive = (self.concurrency == 0) and not self.eos_enabled
        worker_exit_flags = [asyncio.Event() for _ in range(worker_pool_size)]
        self._worker_exit_flags = worker_exit_flags
        self._active_worker_count = worker_pool_size

        async def producer():
            try:
                while not self._is_shutdown_requested(shutdown_event):
                    self._producer_heartbeat_ts = time.time()
                    # Backpressure: if the source (circuit breaker tripped) or
                    # the sink (Kafka down) is unhealthy, stop pulling new
                    # cursors so we don't generate a flood of doomed work that
                    # would otherwise peg CPU / memory / disk. Resume once the
                    # breaker half-opens or the sink cooldown elapses.
                    if self._should_pause_ingestion(shutdown_event):
                        await asyncio.sleep(0.2)
                        continue
                    cursor = await self._next_cursor_or_shutdown(cursor_source, shutdown_event)
                    if cursor is None:
                        break
                    if self._is_shutdown_requested(shutdown_event):
                        break
                    if self.watermark_manager is not None:
                        await self.watermark_manager.mark_emitted(cursor)
                    queued = await self._queue_put_or_shutdown(queue, cursor, shutdown_event)
                    if not queued:
                        break
            finally:
                # If shutdown was requested, workers will exit through the
                # shutdown-aware queue getter. Otherwise use sentinels to stop
                # workers after the bounded source is exhausted.
                if not self._is_shutdown_requested(shutdown_event):
                    for _ in range(worker_pool_size):
                        await queue.put(None)

        async def worker(idx: int):
            # Cooperative exit: when the scheduler shrinks current_limit in
            # adaptive mode, the engine marks the corresponding exit flag and
            # this worker exits cleanly after its current cursor (no mid-RPC
            # cancellation).
            while not worker_exit_flags[idx].is_set():
                self._worker_heartbeat_ts = time.time()
                cursor = await self._queue_get_or_shutdown(queue, shutdown_event)
                if cursor is None:
                    break
                # Sink backpressure at the cursor boundary: acquire a permit
                # before touching the sink so at most sink_inflight_cursors
                # cursors are ever enqueued-but-not-delivery-confirmed. When
                # Kafka drains slower than the engine fetches, workers block
                # here, the engine queue fills, and the producer stalls -- the
                # sink's real capacity throttles the pipeline. Without this,
                # unbounded in-flight cursors saturate the sink queue and every
                # queued delivery times out at sink_failure_timeout_sec even
                # though the sink is healthy (live incident: persistent
                # engine.sink_delivery_failed storms while the sink drained at
                # full speed).
                await sink_gate.acquire()
                gate_held = True
                try:
                    try:
                        success, delivery_futures, expected_watermark, delivery_entities = await self._run_one(cursor)
                    except Exception as exc:
                        # A worker must never die from a single cursor's failure
                        # path. Live incident: when the sink queue was saturated,
                        # an uncaught enqueue TimeoutError from _send_dlq escaped
                        # _run_one and killed every worker; with no consumer left,
                        # the engine cursor queue filled and producer() hung on
                        # queue.put forever -- a silent deadlock (only the
                        # watermark refresh loop kept logging). Log it, then let
                        # _finalize_checkpoint below record the cursor as failed
                        # (it already guards all of its own sink writes).
                        success = False
                        delivery_futures = []
                        expected_watermark = None
                        delivery_entities = []
                        if self.logger:
                            self.logger.error(
                                "engine.worker_cursor_error",
                                cursor=cursor,
                                error=repr(exc),
                                error_type=type(exc).__name__,
                            )
                    # Finalize runs as a task (it waits for delivery) and owns
                    # the sink permit: it releases the gate when done.
                    task = asyncio.create_task(
                        self._finalize_and_release_gate(
                            sink_gate,
                            cursor,
                            success,
                            delivery_futures,
                            expected_watermark=expected_watermark,
                            delivery_entities=delivery_entities,
                        )
                    )
                    self._checkpoint_tasks.add(task)
                    task.add_done_callback(self._checkpoint_tasks.discard)
                    gate_held = False
                finally:
                    if gate_held:
                        sink_gate.release()

        listener = None
        try:
            workers = [
                asyncio.create_task(worker(i))
                for i in range(worker_pool_size)
            ]

            # Register scheduler callback for adaptive shrink.
            if is_adaptive:
                scheduler = getattr(self.fetcher, "scheduler", None)

                def _on_limit_change(new_limit: int):
                    if not worker_exit_flags:
                        return
                    target = max(1, min(int(new_limit), worker_pool_size))
                    active = [
                        i for i, flag in enumerate(worker_exit_flags)
                        if not flag.is_set()
                    ]
                    active_count = len(active)
                    if target >= active_count:
                        # Grow is free: workers were pre-spawned, just update gauge.
                        self._active_worker_count = active_count
                        return
                    for i in active[target:]:
                        worker_exit_flags[i].set()
                    self._active_worker_count = target
                    if self.logger:
                        self.logger.debug(
                            "engine.worker_pool_shrunk",
                            target=target,
                            inflight_limit=int(new_limit),
                            pool_size=worker_pool_size,
                        )

                listener = _on_limit_change
                if scheduler is not None:
                    scheduler.add_window_change_listener(listener)
                    # Fire once with the initial value so the gauge / state
                    # is consistent from the very first observation.
                    listener(scheduler.current_limit)

            await producer()
            if self._is_shutdown_requested(shutdown_event) and self.logger:
                self.logger.warn(
                    "engine.shutdown_draining",
                    queued_blocks=queue.qsize(),
                    checkpoint_tasks=len(self._checkpoint_tasks),
                )
            await asyncio.gather(*workers)
        except asyncio.CancelledError:
            if self.logger:
                self.logger.warn(
                    "engine.shutdown_cancelled",
                )
            for task in workers:
                task.cancel()
            if workers:
                await asyncio.gather(*workers, return_exceptions=True)
        finally:
            # Set every exit flag (even ones already set) and unregister the
            # scheduler listener so a subsequent run_stream() on the same
            # engine instance starts with a clean slate.
            for flag in worker_exit_flags:
                flag.set()
            if listener is not None:
                scheduler = getattr(self.fetcher, "scheduler", None)
                if scheduler is not None:
                    scheduler.remove_window_change_listener(listener)
            if self._checkpoint_tasks:
                # Belt-and-suspenders: individual checkpoint tasks bound their
                # own waits, but if any delivery future is permanently
                # unresolvable (sink worker crash after enqueue but before
                # produce(), no-timeout await), the gather hangs forever.
                # 120s covers sink_failure_timeout_sec (30s) × several retries
                # plus the DLQ/state write attempts.
                try:
                    await asyncio.wait_for(
                        asyncio.gather(*self._checkpoint_tasks, return_exceptions=True),
                        timeout=120.0,
                    )
                except asyncio.TimeoutError:
                    if self.logger:
                        self.logger.warn(
                            "engine.checkpoint_gather_timeout",
                            pending=sum(1 for t in self._checkpoint_tasks if not t.done()),
                        )
                    for task in self._checkpoint_tasks:
                        task.cancel()
                    with suppress(asyncio.CancelledError, Exception):
                        await asyncio.gather(*self._checkpoint_tasks, return_exceptions=True)
            if self.watermark_manager is not None and checkpoint_started:
                status = "completed" if getattr(self.pipeline, "mode", None) == "backfill" else "running"
                try:
                    await asyncio.wait_for(
                        self.watermark_manager.stop(status=status),
                        timeout=60.0,
                    )
                except asyncio.TimeoutError:
                    if self.logger:
                        self.logger.warn("engine.watermark_stop_timeout")
            if sink_started:
                await self.sink.close()

    def _is_shutdown_requested(self, shutdown_event: asyncio.Event | None) -> bool:
        return shutdown_event is not None and shutdown_event.is_set()

    def _should_pause_ingestion(self, shutdown_event: asyncio.Event | None) -> bool:
        if self._is_shutdown_requested(shutdown_event):
            return True
        now = time.monotonic()
        # Source-side: scheduler circuit breaker is open (upstream unhealthy).
        scheduler = getattr(self.fetcher, "scheduler", None)
        source_tripped = scheduler is not None and scheduler.is_tripped()
        # Sink-side: Kafka (or other sink) is unavailable; pause until cooldown.
        sink_unhealthy = (
            self._sink_unhealthy_until is not None and now < self._sink_unhealthy_until
        )
        if source_tripped or sink_unhealthy:
            self._log_ingestion_paused(
                scheduler=scheduler,
                source_tripped=source_tripped,
                sink_unhealthy=sink_unhealthy,
                now=now,
            )
            return True
        return False

    def _log_ingestion_paused(self, *, scheduler, source_tripped, sink_unhealthy, now):
        # Throttled so a real stall is diagnosable from plain logs (no need to
        # attach py-spy) without spamming every 0.2s producer poll.
        if not self.logger:
            return
        if now - self._last_pause_log_ts < self.pause_log_interval_sec:
            return
        self._last_pause_log_ts = now
        fields = {
            "source_breaker_tripped": source_tripped,
            "sink_unhealthy": sink_unhealthy,
        }
        if sink_unhealthy:
            fields["sink_cooldown_remaining_sec"] = round(
                self._sink_unhealthy_until - now, 1
            )
        if scheduler is not None:
            fields["breaker_state"] = getattr(scheduler, "cb_state", None)
            fields["breaker_attempt"] = getattr(scheduler, "cb_attempt", None)
            cooldown_until = getattr(scheduler, "cb_cooldown_until", None)
            if cooldown_until is not None:
                fields["breaker_cooldown_remaining_sec"] = round(
                    max(cooldown_until - now, 0.0), 1
                )
        self.logger.warn("engine.ingestion_paused", **fields)

    def _mark_sink_unhealthy(self):
        self._sink_unhealthy_until = time.monotonic() + self.sink_cooldown_sec

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

    async def _queue_put_or_shutdown(self, queue: asyncio.Queue, item, shutdown_event: asyncio.Event | None) -> bool:
        if shutdown_event is None:
            await queue.put(item)
            return True

        put_task = asyncio.create_task(queue.put(item))
        shutdown_task = asyncio.create_task(shutdown_event.wait())
        done, pending = await asyncio.wait(
            {put_task, shutdown_task},
            return_when=asyncio.FIRST_COMPLETED,
        )

        if shutdown_task in done:
            put_task.cancel()
            with suppress(asyncio.CancelledError):
                await put_task
            return False

        shutdown_task.cancel()
        with suppress(asyncio.CancelledError):
            await shutdown_task
        await put_task
        return True

    async def _queue_get_or_shutdown(self, queue: asyncio.Queue, shutdown_event: asyncio.Event | None):
        if shutdown_event is None:
            return await queue.get()

        get_task = asyncio.create_task(queue.get())
        shutdown_task = asyncio.create_task(shutdown_event.wait())
        done, pending = await asyncio.wait(
            {get_task, shutdown_task},
            return_when=asyncio.FIRST_COMPLETED,
        )

        if shutdown_task in done:
            get_task.cancel()
            with suppress(asyncio.CancelledError):
                await get_task
            return None

        shutdown_task.cancel()
        with suppress(asyncio.CancelledError):
            await shutdown_task
        return await get_task


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
            "decode_ms": 0.0,
            "enrich_ms": 0.0,
            "sink_enqueue_ms": 0.0,
            "sink_delivery_ms": 0.0,
        }
        current_entity = "unknown"
        success = True
        delivery_futures = []
        delivery_entities = []
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
            "message_count": 0,
        }
        attempt = 1
        try:
            with self._tracer.start_as_current_span("streaming.run") as root_span:
                root_span.set_attribute("component", "engine")
                root_span.set_attribute("cursor", cursor)
                root_span.set_attribute("cursor_value", cursor)

                self.metrics.INFLIGHT.add(1)

                while True:
                    success = True
                    parsed_bundle = {}
                    delivery_futures = []
                    delivery_entities = []
                    expected_watermark = None
                    transactional_topic_rows = []
                    retry_requested = False
                    retry_error = None
                    retry_entity = None
                    retry_meta = None
                    current_entity = "unknown"

                    try:
                        # 1. FETCH
                        fetch_started = time.perf_counter()
                        raw_data = await self.fetcher.fetch(cursor)
                        phase_timings["fetch_ms"] = (time.perf_counter() - fetch_started) * 1000

                        for entity, processor in self.processors.items():
                            current_entity = entity
                            rpc_result = raw_data[entity]
                            if isinstance(rpc_result, RpcErrorResult):
                                error_msg = rpc_result.error
                                error_details = rpc_result.details.copy()
                                self.metrics.ERROR_COUNTER.add(1, {"stage": "rpc"})

                                if self.logger:
                                    self.logger.warn(
                                        "engine.rpc_failed",
                                        entity=entity,
                                        cursor=cursor,
                                        error=error_msg,
                                        expected=rpc_result.expected,
                                        **{
                                            key: value
                                            for key, value in error_details.items()
                                            if key != "block"
                                        },
                                    )

                                if rpc_result.expected and self._should_retry_upstream_not_ready(
                                    attempt=attempt,
                                ):
                                    retry_requested = True
                                    retry_error = rpc_result
                                    retry_entity = entity
                                    retry_meta = rpc_result.meta.extra
                                    break

                                await self._send_dlq(
                                    entity=entity,
                                    cursor=cursor,
                                    stage="rpc",
                                    error_type="RpcError",
                                    error_message=error_msg,
                                    payload=None,
                                    context={
                                        "request": rpc_result.meta.extra,
                                        "rpc_error": error_details,
                                        "expected": rpc_result.expected,
                                    },
                                )
                                success = False
                                return False, delivery_futures, expected_watermark, delivery_entities

                            try:
                                value, meta = rpc_result
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
                                ingestion_lag_ms = self._compute_ingestion_lag_ms(
                                    parsed_bundle
                                )
                                if ingestion_lag_ms is not None:
                                    self.metrics.INGESTION_LAG_MS.record(ingestion_lag_ms)
                                if self.logger:
                                    self.logger.info(
                                        "engine.processed",
                                        cursor=cursor,
                                        entity=entity,
                                        **({"rpc_latency_ms": latency} if latency else {}),
                                        payload=emitted_rows,
                                        ingestion_lag_ms=ingestion_lag_ms,
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
                                return False, delivery_futures, expected_watermark, delivery_entities

                        if retry_requested:
                            retry_delay = self._upstream_not_ready_retry_delay_seconds(attempt)
                            if self.logger:
                                self.logger.warn(
                                    "engine.rpc_retry_scheduled",
                                    entity=retry_entity,
                                    cursor=cursor,
                                    attempt=attempt,
                                    max_attempts=self.upstream_not_ready_max_attempts,
                                    retry_delay_ms=round(retry_delay * 1000, 2),
                                    error=retry_error.error if retry_error is not None else None,
                                    **(
                                        {
                                            key: value
                                            for key, value in (retry_error.details or {}).items()
                                            if key != "block"
                                        }
                                        if retry_error is not None
                                        else {}
                                    ),
                                )

                            await asyncio.sleep(retry_delay)
                            attempt += 1
                            continue

                        if success:
                            decode_started = time.perf_counter()
                            if self.decoder is not None:
                                decoded_bundle = await self.decoder.decode(parsed_bundle)
                            else:
                                decoded_bundle = parsed_bundle
                            phase_timings["decode_ms"] += (time.perf_counter() - decode_started) * 1000

                            enrich_started = time.perf_counter()
                            if self.enricher is not None:
                                final_bundle = self.enricher.enrich(decoded_bundle)
                            else:
                                final_bundle = decoded_bundle
                            phase_timings["enrich_ms"] += (time.perf_counter() - enrich_started) * 1000
                            cursor_observation["event_timestamp_ms"] = self._extract_event_timestamp_ms(
                                final_bundle
                            )
                            # For pre-encoded path, ingest_timestamp_ms comes from __meta__
                            # (delivery tracker can't see it because rows are opaque tuples).
                            meta_list = final_bundle.get("__meta__")
                            if isinstance(meta_list, list) and meta_list and isinstance(meta_list[0], dict):
                                ingest_ts = meta_list[0].get("ingest_timestamp_ms")
                                if ingest_ts is not None:
                                    cursor_observation["ingest_timestamp_ms"] = int(ingest_ts)
                            emitted_rows = 0
                            for entity, topic in self.topics.items():
                                rows = final_bundle.get(entity, [])
                                self.metrics.ROW_COUNTER.add(len(rows), {"entity": entity})
                                if not rows:
                                    continue
                                emitted_rows += len(rows)
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
                                        delivery_entities.append((entity, topic))
                                phase_timings["sink_enqueue_ms"] += (time.perf_counter() - sink_started) * 1000
                            receipt_rows = (
                                len(final_bundle.get("receipt", []))
                                if "receipt" not in self.topics
                                else 0
                            )
                            cursor_observation["message_count"] = emitted_rows + receipt_rows
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
                        return False, delivery_futures, expected_watermark, delivery_entities

                    if success:
                        break

                    if attempt < self.upstream_not_ready_max_attempts:
                        attempt += 1
                        continue

                    if retry_error is not None and retry_entity is not None:
                        await self._send_dlq(
                            entity=retry_entity,
                            cursor=cursor,
                            stage="rpc",
                            error_type="RpcError",
                            error_message=retry_error.error,
                            payload=None,
                            context={
                                "request": retry_meta,
                                "rpc_error": retry_error.details,
                                "expected": retry_error.expected,
                                "retry_attempts": attempt,
                            },
                        )
                    return False, delivery_futures, expected_watermark, delivery_entities

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
                        delivery_entities.append(("transaction", None))
                return success, delivery_futures, expected_watermark, delivery_entities
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

    def _should_retry_upstream_not_ready(self, *, attempt: int) -> bool:
        return attempt < self.upstream_not_ready_max_attempts

    def _upstream_not_ready_retry_delay_seconds(self, attempt: int) -> float:
        base_seconds = self._chain_block_time_seconds()
        return max(base_seconds * max(attempt, 1), 0.1)

    def _chain_block_time_seconds(self) -> float:
        interval = getattr(self.chain, "interval_seconds", None)
        if interval is not None:
            try:
                return max(float(interval), 0.1)
            except (TypeError, ValueError):
                pass

        chain_name = getattr(self.chain, "name", None)
        network = getattr(self.chain, "network", None)
        if chain_name and network:
            try:
                return max(get_chain_profile(str(chain_name), str(network)).interval_seconds, 0.1)
            except Exception:
                pass

        network_label = getattr(self.chain, "network_label", None)
        if isinstance(network_label, str) and "-" in network_label:
            chain_name, network = network_label.split("-", 1)
            try:
                return max(get_chain_profile(chain_name, network).interval_seconds, 0.1)
            except Exception:
                pass

        return 1.0

    async def _finalize_and_release_gate(
        self,
        gate: asyncio.Semaphore,
        cursor,
        success,
        delivery_futures,
        *,
        expected_watermark=None,
        delivery_entities=None,
    ):
        """Run _finalize_checkpoint (including its delivery wait) and release
        the sink in-flight permit afterwards -- the permit must outlive the
        whole delivery window so the sink gate actually bounds cursors that
        are still enqueued-but-unconfirmed."""
        try:
            if self.watermark_manager is not None:
                await self._finalize_checkpoint(
                    cursor,
                    success,
                    delivery_futures,
                    expected_watermark=expected_watermark,
                    delivery_entities=delivery_entities,
                )
        finally:
            gate.release()

    async def _finalize_checkpoint(
        self,
        cursor,
        success,
        delivery_futures,
        *,
        expected_watermark=None,
        delivery_entities=None,
    ):
        if not success:
            await self._record_failed_watermark_state(cursor)
            return
        try:
            delivery_results = []
            if delivery_futures:
                wait_started = time.perf_counter()
                try:
                    delivery_results = await asyncio.wait_for(
                        asyncio.gather(*delivery_futures, return_exceptions=True),
                        timeout=self.sink_failure_timeout_sec,
                    )
                except asyncio.TimeoutError:
                    delivery_results = None
                self.metrics.SINK_DELIVERY_WAIT.record(
                    (time.perf_counter() - wait_started) * 1000,
                    {"outcome": "timeout" if delivery_results is None else "success"},
                )
                sink_failed = delivery_results is None or any(
                    isinstance(r, Exception) for r in (delivery_results or [])
                )
                if sink_failed:
                    # Sink (Kafka) is unavailable: mark it unhealthy (the
                    # producer loop will pause) and record the cursor as failed
                    # WITHOUT advancing the watermark. Bounded wait only.
                    self._mark_sink_unhealthy()
                    if self.logger:
                        self.logger.warn(
                            "engine.sink_delivery_failed",
                            cursor=cursor,
                            timed_out=delivery_results is None,
                        )
                    # Sink-delivery failures never used to reach the DLQ --
                    # only processor/rpc-stage failures did -- so a cursor
                    # that failed here left zero forensic trail (no payload,
                    # no error detail) beyond a bare "failed" watermark
                    # state entry. Live incident: a KafkaException
                    # (MSG_SIZE_TOO_LARGE) killed the sink worker and we had
                    # no record of which entity/topic actually produced the
                    # oversized message.
                    await self._send_sink_failure_dlq(
                        cursor,
                        delivery_entities or [],
                        delivery_results,
                    )
                    await self._record_failed_watermark_state(cursor)
                    return
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
                    checkpoint_result = await asyncio.wait_for(
                        checkpoint_future, timeout=self.sink_failure_timeout_sec
                    )
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
                    cursor=cursor,
                    expected=expected_watermark,
                    actual=advanced_watermark,
                )
        except Exception as exc:
            await self._record_failed_watermark_state(cursor, error=str(exc))
            return

    async def _send_sink_failure_dlq(self, cursor, delivery_entities, delivery_results):
        """Best-effort: the sink may already be dead (that's exactly why
        we're here), so a failure to write the DLQ record itself must not
        raise and mask the original failure.

        One DLQ record per cursor, never one per failed entity: retry
        (retry_dlq_record/_run_one) always reprocesses and resinks the
        *whole* cursor, not just the entity named in the record it was
        handed. A cursor whose entities all time out together (e.g. every
        delivery stalling under producer backpressure) used to get one DLQ
        record per entity, so retrying each of those N records replayed the
        full cursor N times -- live incident: a 4-entity cursor produced 4
        duplicate rows in every one of its Kafka topics, including
        bsc.raw_block. All failed entities/topics are preserved in
        `context.failures` for the same forensic detail as before.
        """
        if not delivery_entities:
            return
        timed_out = delivery_results is None
        failures = []
        for i, (entity, topic) in enumerate(delivery_entities):
            result = None if timed_out else delivery_results[i] if i < len(delivery_results) else None
            if not timed_out and not isinstance(result, Exception):
                continue
            error = (
                TimeoutError(f"sink delivery timed out after {self.sink_failure_timeout_sec}s")
                if timed_out
                else result
            )
            failures.append({"entity": entity, "topic": topic, "error_type": type(error).__name__, "error_message": str(error)})

        if not failures:
            return

        try:
            await self._send_dlq(
                entity=",".join(failure["entity"] for failure in failures),
                cursor=cursor,
                stage="sink",
                error_type=failures[0]["error_type"],
                error_message="; ".join(f"{f['entity']}: {f['error_message']}" for f in failures),
                payload=None,
                context={"failures": failures},
            )
        except Exception:
            pass

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
            try:
                delivery_future = await self.sink.send_transaction([(self.watermark_manager.state_topic, [row])])
                if delivery_future is not None:
                    await asyncio.wait_for(delivery_future, timeout=self.sink_failure_timeout_sec)
            except Exception:
                # Sink down: best-effort only; ingestion is already paused. send()
                # itself can raise (queue-full enqueue timeout, dead worker) just
                # like the wait_for below it, so both need to land in this except.
                pass
            return
        try:
            checkpoint_future = await self.sink.send(
                self.watermark_manager.state_topic,
                [row],
                wait_delivery=True,
            )
            if checkpoint_future is not None:
                await asyncio.wait_for(checkpoint_future, timeout=self.sink_failure_timeout_sec)
        except Exception:
            # Sink down: best-effort only; ingestion is already paused. send()
            # itself can raise (queue-full enqueue timeout, dead worker) just
            # like the wait_for below it, so both need to land in this except.
            pass



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
            try:
                delivery_future = await self.sink.send_transaction([(topic, [record])])
            except Exception:
                # Sink down / queue saturated: best-effort only, same rationale
                # as the non-EOS branch below.
                return
            if delivery_future is not None:
                try:
                    await asyncio.wait_for(delivery_future, timeout=self.sink_failure_timeout_sec)
                except (asyncio.TimeoutError, Exception):
                    pass
        else:
            try:
                delivery_future = await self.sink.send(topic, [record])
            except Exception:
                # Best-effort: DLQ is already a fallback path. send() itself can
                # raise (queue-full enqueue timeout, dead worker) just like the
                # wait_for below it, so it must land in the same catch or the
                # exception escapes _run_one and silently kills the worker task
                # -- live incident: with the sink queue saturated, every worker
                # died on this path and the engine deadlocked (cursor queue
                # full, producer blocked on queue.put forever).
                return
            if delivery_future is not None:
                # Best-effort: DLQ is already a fallback path; cap the wait so
                # a stuck delivery future (sink worker crash after enqueue) can't
                # block _finalize_checkpoint or _run_one indefinitely.
                try:
                    await asyncio.wait_for(delivery_future, timeout=self.sink_failure_timeout_sec)
                except (asyncio.TimeoutError, Exception):
                    pass

        if self.logger:
            self.logger.warn(
                "engine.dlq_sent",
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
            success, delivery_futures, expected_watermark, delivery_entities = await self._run_one(record.get("cursor"))
            if self.watermark_manager is not None:
                await self._finalize_checkpoint(
                    record.get("cursor"),
                    success,
                    delivery_futures,
                    expected_watermark=expected_watermark,
                    delivery_entities=delivery_entities,
                )
            self.metrics.DLQ_RETRY_COUNTER.add(
                1,
                {"entity": record.get("entity") or "unknown", "outcome": "success" if success else "failed"},
            )
            return success
        finally:
            self._active_dlq_record = previous

    async def mark_dlq_resolved(self, record: dict) -> None:
        if not self.dlq_topic:
            return
        self.metrics.DLQ_RESOLVED_COUNTER.add(1, {"entity": record.get("entity") or "unknown"})
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
        # Fast path: pre-encoded bundles carry __meta__ with block_timestamp_ms
        # already in milliseconds (set by parse_and_encode_block_envelope in Rust).
        meta_list = bundle.get("__meta__")
        if isinstance(meta_list, list) and meta_list and isinstance(meta_list[0], dict):
            ts_ms = meta_list[0].get("block_timestamp_ms")
            if ts_ms is not None:
                try:
                    return int(ts_ms)
                except Exception:
                    pass

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

    def _compute_ingestion_lag_ms(
        self,
        bundle: dict,
        ingestion_timestamp_ms: int | None = None,
    ) -> int | None:
        block_timestamp_ms = self._extract_event_timestamp_ms(bundle)
        if block_timestamp_ms is None:
            return None
        if ingestion_timestamp_ms is None:
            ingestion_timestamp_ms = int(time.time() * 1000)
        return ingestion_timestamp_ms - block_timestamp_ms
        
    
    # ------------------------------------------------------------------
    # Argo Workflows progress. When a bounded-backfill pod runs under Argo
    # v3.3+, the executor injects ARGO_PROGRESS_FILE (shared /var/run/argo)
    # into the main container and reads its "N/M" content every ~3s, patching
    # the workflow node's progress (UI bar/% on the node and workflow) about
    # once a minute. Real-time processes never have ARGO_PROGRESS_FILE set,
    # so this is a strict no-op for them. Writes are throttled and atomic
    # (tmp + rename) so the executor never reads a partial line, and any
    # filesystem error is swallowed -- progress reporting must never break
    # ingestion. N is derived from the committed contiguous watermark, so it
    # matches the backfill_start/target gauges Grafana plots and is correct
    # across checkpoint resumes.
    # ------------------------------------------------------------------
    _PROGRESS_WRITE_INTERVAL_S = 5.0

    def _write_argo_progress_file(self, done: int, total: int) -> None:
        path = os.environ.get("ARGO_PROGRESS_FILE")
        if not path:
            return
        try:
            tmp = f"{path}.tmp"
            with open(tmp, "w", encoding="utf-8") as fh:
                fh.write(f"{int(done)}/{int(total)}\n")
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, path)
        except OSError:
            return

    def _report_backfill_progress(self) -> None:
        if not os.environ.get("ARGO_PROGRESS_FILE"):
            return
        if self.watermark_manager is None or self.watermark_manager.cursor is None:
            return
        try:
            start = int(getattr(self.pipeline, "start_cursor", None) or 0)
            target = int(getattr(self.pipeline, "end_cursor", None) or 0)
        except (TypeError, ValueError):
            return
        if target <= start:
            return
        committed = int(self.watermark_manager.cursor)
        if committed < start or committed > target:
            return
        total = target - start + 1
        done = committed - start + 1
        now = time.monotonic()
        if done >= total or (now - self._progress_last_ts) >= self._PROGRESS_WRITE_INTERVAL_S:
            self._progress_last_ts = now
            self._write_argo_progress_file(done, total)

    async def _compute_lag(self, cursor):
        head_cursor = None
        head_lag = None
        ingestion_lag = None
        pipeline_mode = getattr(self.pipeline, "mode", None)

        if pipeline_mode == "backfill":
            end_cursor = getattr(self.pipeline, "end_cursor", None)
            if end_cursor is not None:
                # Publish this segment's configured [start, target] bounds as
                # per-instance gauges so dashboards can plot progress % and
                # ETA generically (no hardcoded ranges). Idempotent -- values
                # are static for the run.
                if self.watermark_manager is not None and getattr(
                    self.watermark_manager, "set_backfill_range", None
                ) is not None:
                    start = getattr(self.pipeline, "start_cursor", None)
                    self.watermark_manager.set_backfill_range(
                        start=None if start is None else int(start),
                        target=int(end_cursor),
                    )
                ingestion_lag = max(int(end_cursor) - int(cursor), 0)
                if self.watermark_manager is not None and self.watermark_manager.cursor is not None:
                    self.watermark_manager.update_commit_delay(
                        max(int(end_cursor) - int(self.watermark_manager.cursor), 0)
                    )
                elif self.watermark_manager is not None:
                    self.watermark_manager.update_commit_delay(None)
                self._report_backfill_progress()
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
