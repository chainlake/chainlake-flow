import json
import asyncio
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.protobuf import ProtobufSerializer

class KafkaSink:
    def __init__(self, producer, id_calculator, time_calculator):
        self.producer = producer
        self.id_calc = id_calculator
        self.time_calc = time_calculator

    def send(self, topic, rows):
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
            )

        self.producer.poll(0)

    def flush(self):
        self.producer.flush(10)