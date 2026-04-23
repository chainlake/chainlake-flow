import json
import time
import asyncio
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.protobuf import ProtobufSerializer
from collections import defaultdict
from opentelemetry import metrics
from opentelemetry import trace

tracer = trace.get_tracer(__name__)
meter = metrics.get_meter("kafka")

from rpcstream.metrics.kafka import (
    BATCH_COUNTER,
    BATCH_LATENCY,
    MESSAGE_COUNTER,
    BUFFER_RETRY_COUNTER,
    DELIVERY_SUCCESS,
    DELIVERY_ERROR,
)

class KafkaWriter:
    def __init__(self, producer, id_calculator, time_calculator, logger, config):
        self.producer = producer
        self.id_calc = id_calculator
        self.time_calc = time_calculator
        self.logger = logger

        self.batch_size = config.batch_size
        self.flush_interval = config.flush_interval_ms / 1000
        self.queue_maxsize = config.queue_maxsize

        self.queue = asyncio.Queue(maxsize=self.queue_maxsize)

        self._running = False
        self._worker_task = None

    # ----------------------------
    # Delivery callback
    # ----------------------------
    def delivery_report(self, err, msg):
        if err:
            DELIVERY_ERROR.add(1, {"topic": msg.topic()})
            self.logger.error(
                "kafka.delivery_failed",
                component="sink",
                topic=msg.topic(),
                error=str(err),
            )
        else:
            DELIVERY_SUCCESS.add(1, {"topic": msg.topic()})
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
        with tracer.start_as_current_span("kafka.enqueue") as span:
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

            await asyncio.wait_for(self.queue.put((topic, rows)), timeout=0.1) # batch enqueue, Apply backpressure to engine
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
                topic, rows = item
                buffer.extend((topic, r) for r in rows)

            now = time.time()

            if buffer and (
                len(buffer) >= self.batch_size or
                (now - last_flush) >= self.flush_interval
            ):
                await self._flush_batch(buffer)
                buffer.clear()
                last_flush = now
            
            # Ensure that the producer is regularly polled to send messages
            self.producer.poll(0.1) # Poll frequently to send any messages in the buffer

            # Avoid busy waiting and CPU spinning
            await asyncio.sleep(self.flush_interval / 5)

    # ----------------------------
    # Flush batch
    # ----------------------------
    async def _flush_batch(self, buffer):
        with tracer.start_as_current_span("kafka.batch_send") as span:
            span.set_attribute("component", "sink")
            span.set_attribute("batch_size", len(buffer))
            
            topic_counts = defaultdict(int)

            # count per topic
            for topic, _ in buffer:
                topic_counts[topic] += 1

            if self.logger:
                self.logger.debug(
                    "kafka.batch_send",
                    component="sink",
                    batch_size=len(buffer),
                    topics=dict(topic_counts),
                )
                
            start = time.time()
            BATCH_COUNTER.add(1)
            
            for topic, r in buffer:
                MESSAGE_COUNTER.add(1, {"topic": topic})
                event_id = self.id_calc.calculate_event_id(r)
                
                # fallback for DLQ / unknown schema
                if not event_id:
                    event_id = f"dlq-{r.get('block')}-{time.time_ns()}"

                r["id"] = event_id
                r["ingest_timestamp"] = self.time_calc.calculate_ingest_timestamp()

                self.logger.debug(
                    "kafka.produce_attempt",
                    topic=topic,
                    key=event_id,
                )

                payload = json.dumps(r, separators=(",", ":"))

                retries = 0
                while True:
                    try:
                        self.producer.produce(
                            topic=topic,
                            key=event_id,
                            value=payload,
                            callback=self.delivery_report,
                        )
                        break
                    
                    except BufferError:
                        BUFFER_RETRY_COUNTER.add(1, {"topic": topic})
                        retries += 1
                        if retries > 10:
                            raise RuntimeError("Kafka producer stuck")
                        # backpressure from Kafka (avoid: BufferError: Local: Queue full)
                        self.producer.poll(0.1)
                        await asyncio.sleep(0.01)  # yield to event loop, prevents CPU spinning

            # trigger delivery callbacks
            self.producer.poll(0)
            latency = (time.time() - start) * 1000
            BATCH_LATENCY.record(latency)
            span.set_attribute("batch_latency_ms", latency)
            
    # ----------------------------
    # Lifecycle
    # ----------------------------
    async def start(self):
        self._running = True
        self._worker_task = asyncio.create_task(self._worker())

    async def close(self):
        self._running = False

        if self._worker_task:
            await self._worker_task

        # FORCE FINAL FLUSH
        self.producer.flush()