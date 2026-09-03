import json
import time
import asyncio
from collections import defaultdict

from confluent_kafka import KafkaException
from opentelemetry import trace
from opentelemetry.trace import Link

from rpcstream.metrics.kafka import KafkaMetrics
from rpcstream.runtime.observability.context import ObservabilityContext
from rpcstream.sinks.kafka.protobuf import SchemaRegistrySerializerRegistry

# How often to yield to the asyncio event loop during _flush_batch.
# Yielding per-row was added to prevent event loop starvation (py-spy
# confirmed rpcstream_engine_inflight never exceeded 1 without yields),
# but for high-row-count entities (BSC log: 940 rows/block avg) the
# 1-2ms asyncio scheduling overhead per sleep(0) call accumulated to
# 1-2 seconds of pure overhead per cursor -- preventing the log shard
# from keeping up with BSC 450ms blocks. Yield every N rows: each
# stall window stays under ~0.3ms (10 rows × ~0.03ms Avro/row), which
# is negligible against the 229ms RPC round-trip but reduces asyncio
# overhead by 10x for dense entities.
_FLUSH_YIELD_INTERVAL = 10


class KafkaWriter:
    def __init__(
        self,
        producer,
        id_calculator,
        time_calculator,
        logger,
        config,
        producer_config,
        topic_maps,
        protobuf_enabled=False,
        schema_registry_url=None,
        schema_registry_type: str | None = None,
        protobuf_topic_schemas=None,
        protobuf_auto_register_schemas: bool = True,
        observability: ObservabilityContext | None = None,
        eos_enabled=False,
        eos_init_timeout_sec=30.0,
    ):
        self.producer = producer
        self.id_calc = id_calculator
        self.time_calc = time_calculator
        self.logger = logger
        self.producer_config = producer_config
        self.observability = observability or ObservabilityContext.disabled()
        self._tracer = self.observability.get_tracer(__name__)
        self.metrics = KafkaMetrics(self.observability.get_meter("rpcstream.kafka"))

        self.batch_size = config.batch_size
        self.flush_interval = config.flush_interval_ms / 1000
        self.queue_maxsize = config.queue_maxsize
        # Bounded wait for queue space. A short timeout here turned any
        # sub-second sink hiccup into a failed cursor, which then fed the
        # sink-unhealthy cooldown and the scheduler circuit breaker.
        self.enqueue_timeout_sec = getattr(config, "enqueue_timeout_ms", 2000) / 1000
        self.topic_maps = topic_maps
        self.schema_registry_type = schema_registry_type or (
            "protobuf" if protobuf_enabled else None
        )
        self.protobuf_enabled = self.schema_registry_type == "protobuf"
        self.schema_registry_enabled = self.schema_registry_type is not None
        self.eos_enabled = eos_enabled
        self.eos_init_timeout_sec = eos_init_timeout_sec
        self.protobuf_registry = None

        # How often to log while librdkafka's local queue stays full.
        self.buffer_full_log_interval_sec = 5.0

        self.queue = asyncio.Queue(maxsize=self.queue_maxsize)
        self._queue_depth = 0

        self._running = False
        self._worker_task = None
        self._last_delivery_summary = None

        if self.schema_registry_enabled:
            if not schema_registry_url:
                raise ValueError(
                    "schema registry is enabled but schema registry url is missing; set KAFAK_SCHEMA_REGISTRY"
                )
            self.protobuf_registry = SchemaRegistrySerializerRegistry(
                schema_registry_url=schema_registry_url,
                producer_config=producer_config,
                topic_schemas=protobuf_topic_schemas or {},
                auto_register_schemas=protobuf_auto_register_schemas,
                logger=logger,
                schema_format=self.schema_registry_type,
            )

    # ----------------------------
    # Delivery callback
    # ----------------------------
    def delivery_report(self, err, msg):
        if err:
            self.metrics.DELIVERY_ERROR.add(1, {"topic": msg.topic()})
            if self.logger:
                self.logger.error(
                    "kafka.delivery_failed",
                    topic=msg.topic(),
                    error=str(err),
                )
        else:
            self.metrics.DELIVERY_SUCCESS.add(1, {"topic": msg.topic()})
            if self.logger:
                self.logger.debug(
                    "kafka.delivery_success",
                    topic=msg.topic(),
                    partition=msg.partition(),
                    offset=msg.offset(),
                )

    def _build_delivery_tracker(self, *, topic_rows, wait_delivery: bool):
        if not wait_delivery:
            return None

        event_timestamps = []
        ingest_timestamps = []
        topic_counts = defaultdict(int)
        total_rows = 0
        for topic, rows in topic_rows:
            topic_counts[topic] += len(rows)
            total_rows += len(rows)
            for row in rows:
                event_ts = self.time_calc.calculate_event_timestamp_ms(row)
                if event_ts is not None:
                    event_timestamps.append(event_ts)
                ingest_ts = row.get("ingest_timestamp")
                if ingest_ts is not None:
                    ingest_timestamps.append(int(ingest_ts))

        future = asyncio.get_running_loop().create_future()
        tracker = {
            "future": future,
            "pending": total_rows,
            "started_at_ms": int(time.time() * 1000),
            "ack_count": 0,
            "topic_counts": dict(topic_counts),
            "event_timestamps": event_timestamps,
            "ingest_timestamps": ingest_timestamps,
            "kafka_append_timestamps": [],
        }

        if total_rows == 0:
            future.set_result(
                self._summarize_delivery_tracker(tracker)
            )
        return tracker

    def _summarize_delivery_tracker(self, tracker: dict) -> dict:
        event_timestamp_ms = min(tracker["event_timestamps"]) if tracker["event_timestamps"] else None
        ingest_timestamp_ms = min(tracker["ingest_timestamps"]) if tracker["ingest_timestamps"] else None
        kafka_append_timestamp_ms = (
            max(tracker["kafka_append_timestamps"]) if tracker["kafka_append_timestamps"] else None
        )
        delivery_wait_ms = None
        if tracker.get("started_at_ms") is not None:
            delivery_wait_ms = round((int(time.time() * 1000) - tracker["started_at_ms"]), 2)

        summary = {
            "message_count": tracker.get("ack_count", 0),
            "topic_counts": dict(tracker.get("topic_counts", {})),
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
        }
        self._last_delivery_summary = summary
        return summary

    def _record_delivery_ack(self, delivery_tracker, msg):
        if delivery_tracker is None:
            return

        delivery_tracker["ack_count"] += 1

        kafka_append_timestamp_ms = None
        if msg is not None and hasattr(msg, "timestamp"):
            try:
                _msg_type, kafka_append_timestamp_ms = msg.timestamp()
            except Exception:
                kafka_append_timestamp_ms = None

        if kafka_append_timestamp_ms is not None:
            try:
                kafka_append_timestamp_ms = int(kafka_append_timestamp_ms)
            except Exception:
                kafka_append_timestamp_ms = None

        if kafka_append_timestamp_ms is not None:
            delivery_tracker["kafka_append_timestamps"].append(kafka_append_timestamp_ms)

    # ----------------------------
    # Public API (NON-BLOCKING)
    # ----------------------------
    async def send(self, topic, rows, wait_delivery=False):
        linked_span_context = trace.get_current_span().get_span_context()
        delivery_future = None
        delivery_tracker = self._build_delivery_tracker(
            topic_rows=[(topic, rows)],
            wait_delivery=wait_delivery,
        )
        if delivery_tracker is not None:
            delivery_future = delivery_tracker["future"]

        with self._tracer.start_as_current_span("kafka.enqueue") as span:
            span.set_attribute("component", "sink")
            span.set_attribute("topic", topic)
            span.set_attribute("batch_size", len(rows))
            span.set_attribute("queue_size", self.queue.qsize())
            
            # Log enqueue action before adding to the queue
            if self.logger:
                self.logger.debug(
                    "kafka.enqueue",
                    topic=topic,
                    batch_size=len(rows),
                    queue_size=self.queue.qsize(),
                )

            # A dead sink worker means the queue will never drain again, so
            # waiting out the full timeout only delays an inevitable failure.
            # Fail immediately: the crash itself is already logged by
            # _on_worker_done, and this makes the stall visible to the caller
            # (engine -> Prefect) instead of looking like slow Kafka.
            if self._worker_task is not None and self._worker_task.done():
                raise RuntimeError("kafka sink worker is no longer running")

            # Batch enqueue with real backpressure. The sink queue is the
            # coupling point between the engine's fetch rate and the single
            # sink worker's produce rate: when the worker is slower than the
            # producers, the queue fills and senders must WAIT for a free slot,
            # not time out and fail. Live incident: a 13h-backlog catch-up made
            # the engine fetch far faster than the sink could drain; the old
            # hard enqueue timeout then marked every queued cursor failed,
            # which amplified into DLQ/state-write storms against the same
            # saturated queue. Loop the put until it lands; only a genuinely
            # dead sink worker (checked above each round) is a hard error.
            while True:
                try:
                    await asyncio.wait_for(
                        self.queue.put((topic, rows, linked_span_context, delivery_tracker)),
                        timeout=self.enqueue_timeout_sec,
                    )
                    break
                except asyncio.TimeoutError:
                    if self._worker_task is not None and self._worker_task.done():
                        raise RuntimeError("kafka sink worker is no longer running")
                    continue
            self._queue_depth += 1
            self.metrics.QUEUE_SIZE.add(1)
            span.set_attribute("queue_size_after_enqueue", self._queue_depth)

        return delivery_future

    async def send_checkpoint(self, topic, row, wait_delivery=True):
        return await self.send(topic, [row], wait_delivery=wait_delivery)

    # ----------------------------
    # Worker loop
    # ----------------------------
    async def _worker(self):
        buffer = []
        last_flush = time.time()

        while self._running or not self.queue.empty():
            try:
                item = await asyncio.wait_for(
                    self.queue.get(),
                    timeout=self.flush_interval
                )
            except asyncio.TimeoutError:
                item = None

            if item:
                self._queue_depth = max(self._queue_depth - 1, 0)
                self.metrics.QUEUE_SIZE.add(-1)
                topic, rows, parent_span_context, delivery_tracker = item
                tracker = None
                if delivery_tracker is not None and rows:
                    tracker = delivery_tracker
                buffer.extend((topic, r, parent_span_context, tracker) for r in rows)

            now = time.time()

            if buffer and (
                len(buffer) >= self.batch_size or
                (now - last_flush) >= self.flush_interval
            ):
                await self._flush_buffer(buffer)
                buffer.clear()
                last_flush = now

            # Ensure that the producer is regularly polled to send messages
            self.producer.poll(0) # Poll frequently to send any messages in the buffer

            # Avoid busy waiting and CPU spinning
            await asyncio.sleep(0)

        if buffer:
            await self._flush_buffer(buffer)

    # ----------------------------
    # Flush batch
    # ----------------------------
    async def _flush_buffer(self, buffer):
        await self._flush_batch(buffer)

    async def _flush_batch(self, items):
        links = []
        seen = set()
        for _, _, parent_span_context, _ in items:
            if not parent_span_context.is_valid:
                continue
            key = (parent_span_context.trace_id, parent_span_context.span_id)
            if key in seen:
                continue
            seen.add(key)
            links.append(Link(parent_span_context))

        with self._tracer.start_as_current_span(
            "kafka.batch_send",
            links=links,
        ) as span:
            span.set_attribute("component", "sink")
            span.set_attribute("batch_size", len(items))
            span.set_attribute("linked_span_count", len(links))
            
            topic_counts = defaultdict(int)

            # count per topic
            for topic, _, _, _ in items:
                topic_counts[topic] += 1

            if self.logger:
                self.logger.debug(
                    "kafka.batch_send",
                    batch_size=len(items),
                    topics=dict(topic_counts),
                )
                
            start = time.time()
            self.metrics.BATCH_COUNTER.add(1)
            
            for _row_idx, (topic, r, _, delivery_tracker) in enumerate(items):
                try:
                    kafka_key, payload, event_timestamp_ms, ingest_timestamp_ms = self._prepare_message(topic, r)
                except Exception as exc:
                    self._fail_delivery_tracker(delivery_tracker, exc)
                    raise

                if delivery_tracker is not None:
                    if event_timestamp_ms is not None:
                        delivery_tracker.setdefault("event_timestamps", []).append(event_timestamp_ms)
                    if ingest_timestamp_ms is not None:
                        delivery_tracker.setdefault("ingest_timestamps", []).append(ingest_timestamp_ms)

                if self.logger:
                    self.logger.debug(
                        "kafka.produce_attempt",
                        topic=topic,
                        key=kafka_key,
                    )

                # BufferError means librdkafka's local queue is full
                # (queue.buffering.max.messages). That is ordinary backpressure
                # (broker slower than producer), not a permanent failure, so we
                # wait it out: raising here killed the sink worker, after which
                # the queue was never drained again and every send timed out.
                #
                # A KafkaException here (e.g. MSG_SIZE_TOO_LARGE, which
                # produce() raises synchronously when librdkafka can tell
                # up front a single message won't fit) is specific to this
                # one row, not the worker or the broker connection -- fail
                # just its delivery tracker and move on to the rest of the
                # batch, the same "one bad message must not kill the whole
                # sink forever" fix as the BufferError case above (an
                # oversized "log" entity row hit this live and took down
                # ingestion until the pod was restarted). Deliberately
                # narrower than a bare `except Exception`: a non-Kafka
                # error here means something we didn't anticipate, and
                # test_kafka_sink_worker_crash_is_logged /
                # test_kafka_writer_send_fails_fast_when_worker_is_dead
                # depend on that case still crashing the worker loudly
                # rather than silently swallowing an unknown failure mode.
                try:
                    backpressure_since = None
                    while True:
                        try:
                            self.producer.produce(
                                topic=topic,
                                key=kafka_key,
                                value=payload,
                                callback=self._delivery_callback(delivery_tracker),
                            )
                            break

                        except BufferError:
                            self.metrics.BUFFER_RETRY_COUNTER.add(1, {"topic": topic})
                            now = time.time()
                            if backpressure_since is None:
                                backpressure_since = now
                            elif now - backpressure_since >= self.buffer_full_log_interval_sec:
                                # Log periodically so sustained backpressure is
                                # visible without flooding on every retry.
                                backpressure_since = now
                                if self.logger:
                                    self.logger.warn(
                                        "kafka.producer_backpressure",
                                        topic=topic,
                                    )
                            # Free queued/delivered batches, then yield so the rest
                            # of the event loop keeps running while we wait.
                            self.producer.poll(0.1)
                            await asyncio.sleep(0.01)
                except KafkaException as exc:
                    self._fail_delivery_tracker(delivery_tracker, exc)
                    if self.logger:
                        self.logger.error(
                            "kafka.produce_failed",
                            topic=topic,
                            error=str(exc),
                            error_type=type(exc).__name__,
                        )
                    continue

                # poll(0) is non-blocking and triggers delivery callbacks.
                # Call it after every row so ACKs are never delayed by more
                # than one row's serialization time (prevents the spurious
                # "sink delivery timed out" DLQ storm seen when polling only
                # once per full batch).
                #
                # asyncio.sleep(0) yields to the event loop so RPC-fetch
                # coroutines can run. Yielding per-row caused a different
                # problem for high-row-count entities: BSC log blocks average
                # 940 rows, and with 1-2ms of asyncio scheduling overhead
                # per sleep(0) call under concurrent load, 940 yields added
                # 1-2 seconds of pure scheduling overhead per cursor -- making
                # the log shard structurally unable to keep up with BSC's
                # 450ms blocks. Yield every _FLUSH_YIELD_INTERVAL rows
                # instead: each stall window is ~0.1-0.3ms of Avro work
                # (negligible against 229ms RPC) but asyncio overhead drops
                # from O(rows) to O(rows / _FLUSH_YIELD_INTERVAL).
                self.producer.poll(0)
                if _row_idx % _FLUSH_YIELD_INTERVAL == 0:
                    await asyncio.sleep(0)

            # trigger delivery callbacks
            self.producer.poll(0)
            latency = (time.time() - start) * 1000
            self.metrics.BATCH_LATENCY.record(latency)
            span.set_attribute("batch_latency_ms", latency)
            if self._last_delivery_summary is None:
                self._last_delivery_summary = {
                    "message_count": len(items),
                    "topic_counts": dict(topic_counts),
                    "delivery_wait_ms": latency,
                }

    async def send_transaction(
        self,
        topic_rows,
    ):
        delivery_tracker = self._build_delivery_tracker(
            topic_rows=topic_rows,
            wait_delivery=True,
        )
        if delivery_tracker is None:
            return None

        self._send_transaction_sync(topic_rows, delivery_tracker=delivery_tracker)
        return delivery_tracker["future"]

    def _send_transaction_sync(self, topic_rows, *, delivery_tracker=None):
        if not self.eos_enabled:
            raise RuntimeError("Kafka EOS transaction mode is not enabled")

        self.producer.begin_transaction()
        try:
            for topic, rows in topic_rows:
                for row in rows:
                    kafka_key, payload, event_timestamp_ms, ingest_timestamp_ms = self._prepare_message(topic, row)
                    if delivery_tracker is not None:
                        if event_timestamp_ms is not None:
                            delivery_tracker.setdefault("event_timestamps", []).append(event_timestamp_ms)
                        if ingest_timestamp_ms is not None:
                            delivery_tracker.setdefault("ingest_timestamps", []).append(ingest_timestamp_ms)
                    self.producer.produce(
                        topic=topic,
                        key=kafka_key,
                        value=payload,
                        callback=self._delivery_callback(delivery_tracker),
                    )
                    self.producer.poll(0)
            self.producer.commit_transaction()
            if delivery_tracker is not None and not delivery_tracker["future"].done():
                deadline = time.time() + 5.0
                while not delivery_tracker["future"].done() and time.time() < deadline:
                    self.producer.poll(0.05)
            if delivery_tracker is not None and not delivery_tracker["future"].done():
                delivery_tracker["future"].set_result(self._summarize_delivery_tracker(delivery_tracker))
        except Exception:
            self.producer.abort_transaction()
            raise

    def _prepare_message(self, topic, row):
        self.metrics.MESSAGE_COUNTER.add(1, {"topic": topic})
        partition_key = row.pop("kafka_partition_key", None)
        event_id = row.get("id") or self.id_calc.calculate_event_id(row)
        event_timestamp_ms = self.time_calc.calculate_event_timestamp_ms(row)

        if not event_id:
            event_id = f"dlq-{row.get('cursor')}-{time.time_ns()}"

        row["id"] = event_id
        row["ingest_timestamp"] = self.time_calc.calculate_ingest_timestamp()
        kafka_key = partition_key or event_id

        payload = self._serialize(topic, row)
        return kafka_key, payload, event_timestamp_ms, row["ingest_timestamp"]

    def _delivery_callback(self, delivery_tracker):
        def callback(err, msg):
            self.delivery_report(err, msg)
            if delivery_tracker is None:
                return
            if err:
                self._fail_delivery_tracker(delivery_tracker, RuntimeError(str(err)))
                return
            self._record_delivery_ack(delivery_tracker, msg)
            future = delivery_tracker["future"]
            if future.done():
                return
            delivery_tracker["pending"] -= 1
            if delivery_tracker["pending"] == 0:
                future.set_result(self._summarize_delivery_tracker(delivery_tracker))

        return callback

    def _fail_delivery_tracker(self, delivery_tracker, exc):
        if delivery_tracker is None:
            return
        future = delivery_tracker["future"]
        if not future.done():
            future.set_exception(exc)
            
    # ----------------------------
    # Lifecycle
    # ----------------------------
    async def start(self):
        if self.protobuf_registry is not None:
            warmup_started = time.time()
            if self.logger:
                self.logger.debug(
                    "kafka.schema_registry_warmup_started",
                    schema_registry=self.protobuf_registry.schema_registry_url,
                    schema_registry_type=self.schema_registry_type,
                    topic_count=len(self.protobuf_registry.topic_schemas),
                )
            self.protobuf_registry.start()
            if self.logger:
                self.logger.debug(
                    "kafka.schema_registry_warmup_complete",
                    schema_registry=self.protobuf_registry.schema_registry_url,
                    schema_registry_type=self.schema_registry_type,
                    topic_count=len(self.protobuf_registry.topic_schemas),
                    elapsed_ms=round((time.time() - warmup_started) * 1000, 2),
                )

        if self.eos_enabled:
            if self.logger:
                self.logger.info(
                    "kafka.eos_init_started",
                    transactional_id=self.producer_config.get("transactional.id"),
                    timeout_sec=self.eos_init_timeout_sec,
                )
            await self._init_transactions()
            if self.logger:
                self.logger.info(
                    "kafka.eos_init_complete",
                    transactional_id=self.producer_config.get("transactional.id"),
                )

        self._running = True
        self._worker_task = asyncio.create_task(self._worker())
        # asyncio only reports "Task exception was never retrieved" when the Task
        # is garbage-collected. We keep a strong reference in self._worker_task,
        # so a crashed worker would otherwise die completely silently and the
        # sink would stop draining with no trace in the logs.
        self._worker_task.add_done_callback(self._on_worker_done)

    def _on_worker_done(self, task: asyncio.Task) -> None:
        """Surface a crashed sink worker instead of letting it die silently."""
        if task.cancelled():
            return
        exc = task.exception()
        if exc is None:
            return
        if self.logger:
            self.logger.error(
                "kafka.sink_worker_crashed",
                error=str(exc),
                error_type=type(exc).__name__,
            )

    async def _init_transactions(self):
        attempts = 3
        backoff_sec = 1.0
        last_exc = None

        for attempt in range(1, attempts + 1):
            try:
                # Startup already runs before the engine begins consuming blocks,
                # so a synchronous init keeps teardown deterministic in tests and
                # avoids lingering executor threads.
                self.producer.init_transactions(self.eos_init_timeout_sec)
                return
            except Exception as exc:
                last_exc = exc
                err_text = str(exc)
                retryable = any(
                    token in err_text
                    for token in ("_TIMED_OUT", "TIMED_OUT", "Timed out waiting")
                )

                if self.logger:
                    self.logger.warn(
                        "kafka.eos_init_failed",
                        transactional_id=self.producer_config.get("transactional.id"),
                        attempt=attempt,
                        attempts=attempts,
                        retryable=retryable,
                        error=err_text,
                    )

                if not retryable or attempt >= attempts:
                    raise RuntimeError(
                        "Kafka EOS initialization failed; verify the broker supports "
                        "transactions and that the service user is authorized for the "
                        "transactional.id resource"
                    ) from exc

                await asyncio.sleep(backoff_sec)
                backoff_sec *= 2

        if last_exc is not None:
            raise RuntimeError(
                "Kafka EOS initialization failed after retries"
            ) from last_exc

    async def close(self):
        self._running = False

        if self._worker_task:
            await self._worker_task

        if self._queue_depth > 0:
            self.metrics.QUEUE_SIZE.add(-self._queue_depth)
            self._queue_depth = 0

        # FORCE FINAL FLUSH
        self.producer.flush()

    def _serialize(self, topic, row):
        if self.protobuf_registry is not None:
            return self.protobuf_registry.serialize(topic, row)
        return json.dumps(row, separators=(",", ":"))
