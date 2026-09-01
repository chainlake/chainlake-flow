from __future__ import annotations

from rpcstream.adapters import build_chain_adapter
from rpcstream.sinks.kafka.admin import KafkaTopicManager
from rpcstream.sinks.kafka.protobuf import SchemaRegistrySerializerRegistry

ProtobufSerializerRegistry = SchemaRegistrySerializerRegistry

SCHEMA_REGISTRY_INTERNAL_TOPIC = "_schemas"


def all_topics(topic_maps) -> list[str]:
    topics = []
    topics.extend(topic_maps.main.values())
    if topic_maps.dlq:
        topics.append(topic_maps.dlq)
    if getattr(topic_maps, "checkpoint", None):
        topics.append(topic_maps.checkpoint)
    if getattr(topic_maps, "watermark_state", None):
        topics.append(topic_maps.watermark_state)
    return topics


def business_topics(topic_maps) -> list[str]:
    return list(topic_maps.main.values())


# Sized from live BSC mainnet volume (retained-message counts observed on a
# single-node cluster): token_transfer's decoded-event fan-out is by far the
# heaviest (~6x enriched_transaction, ~3x raw_log), raw_log next, then
# enriched_transaction, then raw_block (one row per block, negligible
# volume). Redpanda is thread-per-core, so more partitions only actually
# parallelizes broker-side writes if the broker also has that many cores
# (see redpanda-values.yaml's resources.cpu.cores). An entity not listed
# here (e.g. a future `trace`) falls through to the broker's default
# partition count.
DEFAULT_ENTITY_PARTITIONS = {
    "token_transfer": 6,
    "log": 4,
    "transaction": 3,
    "block": 2,
}


def business_topic_partitions(topic_maps) -> dict[str, int]:
    return {
        topic: DEFAULT_ENTITY_PARTITIONS[entity]
        for entity, topic in topic_maps.main.items()
        if entity in DEFAULT_ENTITY_PARTITIONS
    }


def system_topics(topic_maps) -> list[str]:
    topics = []
    if topic_maps.dlq:
        topics.append(topic_maps.dlq)
    if getattr(topic_maps, "watermark_state", None):
        topics.append(topic_maps.watermark_state)
    return topics


def bootstrap_kafka_resources(runtime, adapter=None, logger=None) -> None:
    adapter = adapter or build_chain_adapter(runtime.chain.type)
    schema_registry_enabled = getattr(
        runtime.kafka,
        "schema_registry_enabled",
        getattr(runtime.kafka, "protobuf_enabled", False),
    )
    schema_registry_type = getattr(
        runtime.kafka,
        "schema_registry_type",
        "protobuf" if getattr(runtime.kafka, "protobuf_enabled", False) else None,
    )
    topic_manager = KafkaTopicManager(
        producer_config=runtime.kafka.config,
        logger=logger,
    )

    topic_manager.ensure_topics(
        business_topics(runtime.topic_map),
        partitions=business_topic_partitions(runtime.topic_map),
    )
    topic_manager.ensure_topics(system_topics(runtime.topic_map))
    topic_manager.ensure_compacted_topics(
        [runtime.checkpoint.topic, runtime.checkpoint.watermark_state_topic]
    )

    if not schema_registry_enabled:
        if logger:
            logger.info(
                "kafka.bootstrap_complete",
                schema_registry_enabled=False,
                schema_registry_mode="disabled",
                checkpoint_topic=runtime.checkpoint.topic,
                watermark_state_topic=runtime.checkpoint.watermark_state_topic,
            )
        return

    if not runtime.kafka.schema_registry_url:
        raise ValueError(
            f"schema registry mode {schema_registry_type or 'avro'} is enabled but schema registry url is missing; set KAFAK_SCHEMA_REGISTRY or KAFKA_SCHEMA_REGISTRY"
        )

    _ensure_schema_registry_internal_topic(topic_manager, logger=logger)

    registry_kwargs = {
        "schema_registry_url": runtime.kafka.schema_registry_url,
        "producer_config": runtime.kafka.config,
        "topic_schemas": adapter.build_protobuf_topic_schemas(
            topic_maps=runtime.topic_map,
            entities=runtime.entities,
        ),
        "logger": logger,
    }
    if schema_registry_type:
        registry_kwargs["schema_format"] = schema_registry_type
    registry_cls = ProtobufSerializerRegistry
    try:
        protobuf_registry = registry_cls(**registry_kwargs)
    except TypeError:
        registry_kwargs.pop("schema_format", None)
        protobuf_registry = registry_cls(**registry_kwargs)
    protobuf_registry.start()

    if logger:
        logger.info(
            "kafka.bootstrap_complete",
            schema_registry_enabled=True,
            schema_registry_type=schema_registry_type,
            schema_registry_mode=schema_registry_type or "avro",
            topic_count=len(all_topics(runtime.topic_map)),
            schema_topic_count=len(protobuf_registry.topic_schemas),
            checkpoint_topic=runtime.checkpoint.topic,
            watermark_state_topic=runtime.checkpoint.watermark_state_topic,
        )


def _ensure_schema_registry_internal_topic(topic_manager, logger=None) -> None:
    try:
        topic_manager.ensure_compacted_topics([SCHEMA_REGISTRY_INTERNAL_TOPIC])
    except Exception as exc:
        if not _is_topic_authorization_failed(exc):
            raise

        if logger:
            logger.info(
                "kafka.schema_registry_internal_topic_skipped",
                topic=SCHEMA_REGISTRY_INTERNAL_TOPIC,
                reason="topic is protected or alter_configs is not allowed",
            )


def _is_topic_authorization_failed(exc: Exception) -> bool:
    text = str(exc)
    return (
        "TOPIC_AUTHORIZATION_FAILED" in text
        or "kafka_nodelete_topics" in text
        or "kafka_noproduce_topics" in text
        or "alter_configs" in text
    )
