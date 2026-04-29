from __future__ import annotations

from rpcstream.adapters.evm.schema import EVM_ENTITY_SCHEMAS, EntitySchema
from rpcstream.sinks.kafka.admin import KafkaTopicManager
from rpcstream.sinks.kafka.protobuf import CHECKPOINT_SCHEMA, DLQ_SCHEMA, ProtobufSerializerRegistry


def build_protobuf_topic_schemas(topic_maps, entities: list[str]) -> dict[str, EntitySchema]:
    topic_schemas = {
        topic_maps.main[entity]: EVM_ENTITY_SCHEMAS[entity]
        for entity in entities
        if entity in EVM_ENTITY_SCHEMAS
    }
    topic_schemas[topic_maps.dlq] = DLQ_SCHEMA
    if getattr(topic_maps, "checkpoint", None):
        topic_schemas[topic_maps.checkpoint] = CHECKPOINT_SCHEMA
    return topic_schemas


def all_topics(topic_maps) -> list[str]:
    topics = []
    topics.extend(topic_maps.main.values())
    if topic_maps.dlq:
        topics.append(topic_maps.dlq)
    return topics


def bootstrap_kafka_resources(runtime, logger=None) -> None:
    topic_manager = KafkaTopicManager(
        producer_config=runtime.kafka.config,
        logger=logger,
    )
    topic_manager.ensure_topics(all_topics(runtime.topic_map))
    topic_manager.ensure_compacted_topics([runtime.checkpoint.topic])

    if not runtime.kafka.protobuf_enabled:
        if logger:
            logger.info(
                "kafka.bootstrap_complete",
                component="sink",
                protobuf_enabled=False,
                checkpoint_topic=runtime.checkpoint.topic,
            )
        return

    if not runtime.kafka.schema_registry_url:
        raise ValueError(
            "protobuf is enabled but schema registry url is missing; set KAFAK_SCHEMA_REGISTRY"
        )

    protobuf_registry = ProtobufSerializerRegistry(
        schema_registry_url=runtime.kafka.schema_registry_url,
        producer_config=runtime.kafka.config,
        topic_schemas=build_protobuf_topic_schemas(runtime.topic_map, runtime.entities),
        logger=logger,
    )
    protobuf_registry.start()

    if logger:
        logger.info(
            "kafka.bootstrap_complete",
            component="sink",
            protobuf_enabled=True,
            topic_count=len(all_topics(runtime.topic_map)),
            schema_topic_count=len(protobuf_registry.topic_schemas),
            checkpoint_topic=runtime.checkpoint.topic,
        )
