import json
import asyncio
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.protobuf import ProtobufSerializer

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