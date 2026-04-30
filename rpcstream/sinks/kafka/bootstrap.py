from __future__ import annotations

from rpcstream.adapters import build_chain_adapter
from rpcstream.sinks.kafka.admin import KafkaTopicManager
from rpcstream.sinks.kafka.protobuf import ProtobufSerializerRegistry


def all_topics(topic_maps) -> list[str]:
    topics = []
    topics.extend(topic_maps.main.values())
    if topic_maps.dlq:
        topics.append(topic_maps.dlq)
    if getattr(topic_maps, "watermark_state", None):
        topics.append(topic_maps.watermark_state)
    return topics


def bootstrap_kafka_resources(runtime, adapter=None, logger=None) -> None:
    adapter = adapter or build_chain_adapter(runtime.chain.type)
    topic_manager = KafkaTopicManager(
        producer_config=runtime.kafka.config,
        logger=logger,
    )
    topic_manager.ensure_topics(all_topics(runtime.topic_map))
    topic_manager.ensure_compacted_topics(
        [runtime.checkpoint.topic, runtime.checkpoint.watermark_state_topic]
    )

    if not runtime.kafka.protobuf_enabled:
        if logger:
            logger.info(
                "kafka.bootstrap_complete",
                component="sink",
                protobuf_enabled=False,
                checkpoint_topic=runtime.checkpoint.topic,
                watermark_state_topic=runtime.checkpoint.watermark_state_topic,
            )
        return

    if not runtime.kafka.schema_registry_url:
        raise ValueError(
            "protobuf is enabled but schema registry url is missing; set KAFAK_SCHEMA_REGISTRY"
        )

    protobuf_registry = ProtobufSerializerRegistry(
        schema_registry_url=runtime.kafka.schema_registry_url,
        producer_config=runtime.kafka.config,
        topic_schemas=adapter.build_protobuf_topic_schemas(
            topic_maps=runtime.topic_map,
            entities=runtime.entities,
        ),
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
            watermark_state_topic=runtime.checkpoint.watermark_state_topic,
        )
