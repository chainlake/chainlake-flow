
from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.serialization import SerializationContext, MessageField
from typing import Optional, Dict, Any


def load_last_state(
    *,
    state_key: str,
    kafka_broker: str,
    state_topic: str,
    schema_registry_url: str,
) -> Optional[Dict[str, Any]]:
    schema_registry_client = SchemaRegistryClient({
        "url": schema_registry_url,
    })
    deserializer = AvroDeserializer(schema_registry_client)

    consumer = Consumer({
        "bootstrap.servers": kafka_broker,
        "group.id": f"state-reader-{state_key}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "isolation.level": "read_committed",
    })

    consumer.assign([TopicPartition(state_topic, 0)])
    
    target_key = state_key.encode("utf-8")

    latest_state = None

    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            break
        if msg.error():
            continue
        if msg.key() != target_key:
            continue

        latest_state = deserializer(
            msg.value(),
            SerializationContext(state_topic, MessageField.VALUE),
        )

    consumer.close()
    return latest_state
