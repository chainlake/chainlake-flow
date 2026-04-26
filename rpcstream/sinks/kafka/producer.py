import json
import time
import asyncio
from collections import defaultdict

from opentelemetry import trace
from opentelemetry.trace import Link

from rpcstream.metrics.kafka import KafkaMetrics
from rpcstream.runtime.observability.context import ObservabilityContext
from rpcstream.sinks.kafka.protobuf import ProtobufSerializerRegistry

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
        protobuf_topic_schemas=None,
        observability: ObservabilityContext | None = None,
    ):
        self.producer = producer
        self.id_calc = id_calculator
        self.time_calc = time_calculator
        self.logger = logger
        self.observability = observability or ObservabilityContext.disabled()
        self._tracer = self.observability.get_tracer(__name__)
        self.metrics = KafkaMetrics(self.observability.get_meter("rpcstream.kafka"))

        self.batch_size = config.batch_size
        self.flush_interval = config.flush_interval_ms / 1000
        self.queue_maxsize = config.queue_maxsize
        self.topic_maps = topic_maps
        self.protobuf_enabled = protobuf_enabled
        self.protobuf_registry = None

        self.queue = asyncio.Queue(maxsize=self.queue_maxsize)
        self._queue_depth = 0

        self._running = False
        self._worker_task = None

        if self.protobuf_enabled:
            if not schema_registry_url:
                raise ValueError(
                    "protobuf is enabled but schema registry url is missing; set KAFAK_SCHEMA_REGISTRY"
                )
            self.protobuf_registry = ProtobufSerializerRegistry(
                schema_registry_url=schema_registry_url,
                producer_config=producer_config,
                topic_schemas=protobuf_topic_schemas or {},
                auto_register_schemas=False,
                logger=logger,
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
                    component="sink",
                    topic=msg.topic(),
                    error=str(err),
                )
        else:
            self.metrics.DELIVERY_SUCCESS.add(1, {"topic": msg.topic()})
            if self.logger:
                self.logger.debug(
                    "kafka.delivery_success",
                    component="sink",
                    topic=msg.topic(),
                    partition=msg.partition(),
                    offset=msg.offset(),
                )

    # ----------------------------
    # Public API (NON-BLOCKING)
    # ----------------------------
    async def send(self, topic, rows, wait_delivery=False):
        linked_span_context = trace.get_current_span().get_span_context()
        delivery_future = None
        if wait_delivery:
            delivery_future = asyncio.get_running_loop().create_future()
            if not rows:
                delivery_future.set_result(True)

        with self._tracer.start_as_current_span("kafka.enqueue") as span:
            span.set_attribute("component", "sink")
            span.set_attribute("topic", topic)
            span.set_attribute("batch_size", len(rows))
            span.set_attribute("queue_size", self.queue.qsize())
            
            # Log enqueue action before adding to the queue
            if self.logger:
                self.logger.debug(
                    "kafka.enqueue",
                    component="sink",
                    topic=topic,
                    batch_size=len(rows),
                    queue_size=self.queue.qsize(),
                )

            await asyncio.wait_for(
                self.queue.put((topic, rows, linked_span_context, delivery_future)),
                timeout=0.1,
            ) # batch enqueue, Apply backpressure to engine
            self._queue_depth += 1
            self.metrics.QUEUE_SIZE.add(1)
            span.set_attribute("queue_size_after_enqueue", self._queue_depth)

        return delivery_future

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
                topic, rows, parent_span_context, delivery_future = item
                tracker = None
                if delivery_future is not None and rows:
                    tracker = {"future": delivery_future, "pending": len(rows)}
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
                    component="sink",
                    batch_size=len(items),
                    topics=dict(topic_counts),
                )
                
            start = time.time()
            self.metrics.BATCH_COUNTER.add(1)
            
            for topic, r, _, delivery_tracker in items:
                self.metrics.MESSAGE_COUNTER.add(1, {"topic": topic})
                partition_key = r.pop("kafka_partition_key", None)
                event_id = r.get("id") or self.id_calc.calculate_event_id(r)
                
                # fallback for DLQ / unknown schema
                if not event_id:
                    event_id = f"dlq-{r.get('block_number') or r.get('block')}-{time.time_ns()}"

                r["id"] = event_id
                r["ingest_timestamp"] = self.time_calc.calculate_ingest_timestamp()
                kafka_key = partition_key or event_id

                if self.logger:
                    self.logger.debug(
                        "kafka.produce_attempt",
                        topic=topic,
                        key=kafka_key,
                    )

                try:
                    payload = self._serialize(topic, r)
                except Exception as exc:
                    self._fail_delivery_tracker(delivery_tracker, exc)
                    raise

                retries = 0
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
                        retries += 1
                        if retries > 10:
                            self._fail_delivery_tracker(
                                delivery_tracker,
                                RuntimeError("Kafka producer stuck"),
                            )
                            raise RuntimeError("Kafka producer stuck")
                        # backpressure from Kafka (avoid: BufferError: Local: Queue full)
                        self.producer.poll(0.1)
                        await asyncio.sleep(0.01)  # yield to event loop, prevents CPU spinning

            # trigger delivery callbacks
            self.producer.poll(0)
            latency = (time.time() - start) * 1000
            self.metrics.BATCH_LATENCY.record(latency)
            span.set_attribute("batch_latency_ms", latency)

    def _delivery_callback(self, delivery_tracker):
        def callback(err, msg):
            self.delivery_report(err, msg)
            if delivery_tracker is None:
                return
            future = delivery_tracker["future"]
            if future.done():
                return
            loop = future.get_loop()
            if err:
                loop.call_soon_threadsafe(future.set_exception, RuntimeError(str(err)))
                return

            delivery_tracker["pending"] -= 1
            if delivery_tracker["pending"] == 0:
                loop.call_soon_threadsafe(future.set_result, True)

        return callback

    def _fail_delivery_tracker(self, delivery_tracker, exc):
        if delivery_tracker is None:
            return
        future = delivery_tracker["future"]
        if not future.done():
            future.get_loop().call_soon_threadsafe(future.set_exception, exc)
            
    # ----------------------------
    # Lifecycle
    # ----------------------------
    async def start(self):
        if self.protobuf_registry is not None:
            warmup_started = time.time()
            if self.logger:
                self.logger.debug(
                    "kafka.protobuf_warmup_started",
                    component="sink",
                    schema_registry=self.protobuf_registry.schema_registry_url,
                    topic_count=len(self.protobuf_registry.topic_schemas),
                )
            self.protobuf_registry.start()
            if self.logger:
                self.logger.debug(
                    "kafka.protobuf_warmup_complete",
                    component="sink",
                    schema_registry=self.protobuf_registry.schema_registry_url,
                    topic_count=len(self.protobuf_registry.topic_schemas),
                    elapsed_ms=round((time.time() - warmup_started) * 1000, 2),
                )

        self._running = True
        self._worker_task = asyncio.create_task(self._worker())

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
