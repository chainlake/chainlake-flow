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
            self.logger.error(
                "kafka.delivery_failed",
                component="sink",
                topic=msg.topic(),
                error=str(err),
            )
        else:
            self.metrics.DELIVERY_SUCCESS.add(1, {"topic": msg.topic()})
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
    async def send(self, topic, rows):
        linked_span_context = trace.get_current_span().get_span_context()

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
                self.queue.put((topic, rows, linked_span_context)),
                timeout=0.1,
            ) # batch enqueue, Apply backpressure to engine
            # QUEUE_SIZE.set(self.queue.qsize())

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
                topic, rows, parent_span_context = item
                buffer.extend((topic, r, parent_span_context) for r in rows)

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

    # ----------------------------
    # Flush batch
    # ----------------------------
    async def _flush_buffer(self, buffer):
        await self._flush_batch(buffer)

    async def _flush_batch(self, items):
        links = []
        seen = set()
        for _, _, parent_span_context in items:
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
            for topic, _, _ in items:
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
            
            for topic, r, _ in items:
                self.metrics.MESSAGE_COUNTER.add(1, {"topic": topic})
                partition_key = r.pop("kafka_partition_key", None)
                event_id = r.get("id") or self.id_calc.calculate_event_id(r)
                
                # fallback for DLQ / unknown schema
                if not event_id:
                    event_id = f"dlq-{r.get('block_number') or r.get('block')}-{time.time_ns()}"

                r["id"] = event_id
                r["ingest_timestamp"] = self.time_calc.calculate_ingest_timestamp()
                kafka_key = partition_key or event_id

                self.logger.debug(
                    "kafka.produce_attempt",
                    topic=topic,
                    key=kafka_key,
                )

                payload = self._serialize(topic, r)

                retries = 0
                while True:
                    try:
                        self.producer.produce(
                            topic=topic,
                            key=kafka_key,
                            value=payload,
                            callback=self.delivery_report,
                        )
                        break
                    
                    except BufferError:
                        self.metrics.BUFFER_RETRY_COUNTER.add(1, {"topic": topic})
                        retries += 1
                        if retries > 10:
                            raise RuntimeError("Kafka producer stuck")
                        # backpressure from Kafka (avoid: BufferError: Local: Queue full)
                        self.producer.poll(0.1)
                        await asyncio.sleep(0.01)  # yield to event loop, prevents CPU spinning

            # trigger delivery callbacks
            self.producer.poll(0)
            latency = (time.time() - start) * 1000
            self.metrics.BATCH_LATENCY.record(latency)
            span.set_attribute("batch_latency_ms", latency)
            
    # ----------------------------
    # Lifecycle
    # ----------------------------
    async def start(self):
        if self.protobuf_registry is not None:
            warmup_started = time.time()
            if self.logger:
                self.logger.info(
                    "kafka.protobuf_warmup_started",
                    component="sink",
                    schema_registry=self.protobuf_registry.schema_registry_url,
                    topic_count=len(self.protobuf_registry.topic_schemas),
                )
            await asyncio.to_thread(self.protobuf_registry.start)
            if self.logger:
                self.logger.info(
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

        # FORCE FINAL FLUSH
        self.producer.flush()

    def _serialize(self, topic, row):
        if self.protobuf_registry is not None:
            return self.protobuf_registry.serialize(topic, row)
        return json.dumps(row, separators=(",", ":"))
