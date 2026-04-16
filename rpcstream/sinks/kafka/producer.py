import json
import time
import asyncio
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.protobuf import ProtobufSerializer
from collections import defaultdict

class KafkaSink:
    def __init__(self, producer, id_calculator, time_calculator, logger):
        self.producer = producer
        self.id_calc = id_calculator
        self.time_calc = time_calculator
        self.logger = logger

    def delivery_report(self, err, msg, *args):
        if err is not None:
            self.logger.error(
                "kafka.delivery_failed",
                component="sink",
                topic=msg.topic(),
                error=str(err),
            )
        else:
            self.logger.info(
                "kafka.delivery_success",
                component="sink",
                topic=msg.topic(),
                partition=msg.partition(),
                offset=msg.offset(),
            )

    def send(self, topic, rows):
        if self.logger:
            self.logger.info(
                "kafka.send",
                component="sink",
                topic=topic,
                batch_size=len(rows)
            )
            
        for r in rows:
            event_id = self.id_calc.calculate_event_id(r)
            if not event_id:
                continue

            r["id"] = event_id
            r["event_timestamp"] = self.time_calc.calculate_event_timestamp(r)
            r["ingest_timestamp"] = self.time_calc.calculate_ingest_timestamp()

            self.producer.produce(
                topic=topic,
                key=event_id,
                value=json.dumps(r),
                callback=self.delivery_report
            )

        self.producer.poll(0)

    def flush(self):
        self.producer.flush()
        
        
class KafkaWriter:
    def __init__(self, producer, id_calculator, time_calculator, logger, config):
        self.producer = producer
        self.id_calc = id_calculator
        self.time_calc = time_calculator
        self.logger = logger

        streaming = config["kafka"].get("streaming", {})

        self.batch_size = streaming.get("batch_size", 500)
        self.flush_interval = streaming.get("flush_interval_ms", 50) / 1000
        self.queue_maxsize = streaming.get("queue_maxsize", 10000)

        self.queue = asyncio.Queue(maxsize=self.queue_maxsize)

        self._running = False
        self._worker_task = None

    # ----------------------------
    # Delivery callback
    # ----------------------------
    def delivery_report(self, err, msg):
        if err:
            self.logger.error(
                "kafka.delivery_failed",
                component="sink",
                topic=msg.topic(),
                error=str(err),
            )
        else:
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
        if self.logger:
            self.logger.debug(
                "kafka.enqueue",
                component="sink",
                topic=topic,
                batch_size=len(rows),
                queue_size=self.queue.qsize(),
            )

        for r in rows:
            await self.queue.put((topic, r))  # async enqueue

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
                buffer.append(item)

            except asyncio.TimeoutError:
                pass

            now = time.time()

            if buffer and (
                len(buffer) >= self.batch_size or
                (now - last_flush) >= self.flush_interval
            ):
                self._flush_batch(buffer)
                buffer.clear()
                last_flush = now

            self.producer.poll(0)

    # ----------------------------
    # Flush batch
    # ----------------------------
    def _flush_batch(self, buffer):
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

        for topic, r in buffer:
            event_id = self.id_calc.calculate_event_id(r)
            if not event_id:
                continue

            r["id"] = event_id
            r["event_timestamp"] = self.time_calc.calculate_event_timestamp(r)
            r["ingest_timestamp"] = self.time_calc.calculate_ingest_timestamp()

            self.producer.produce(
                topic=topic,
                key=event_id,
                value=json.dumps(r),
                callback=self.delivery_report,
            )

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

        # final flush
        self.producer.flush()