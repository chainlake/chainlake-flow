from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping


@dataclass(frozen=True)
class FieldSchema:
    name: str
    scalar_type: str
    repeated: bool = False


@dataclass(frozen=True)
class EntitySchema:
    entity: str
    message_name: str
    fields: tuple[FieldSchema, ...]
    package: str = "rpcstream.system"


def build_system_topic_schemas(topic_maps) -> dict[str, EntitySchema]:
    schemas = {
        topic_maps.dlq: DLQ_SCHEMA,
    }
    if getattr(topic_maps, "checkpoint", None):
        schemas[topic_maps.checkpoint] = CHECKPOINT_SCHEMA
    if getattr(topic_maps, "watermark_state", None):
        schemas[topic_maps.watermark_state] = WATERMARK_STATE_SCHEMA
    return schemas


def build_entity_topic_schemas(
    topic_maps,
    entity_schemas: Mapping[str, EntitySchema],
    entities: list[str],
) -> dict[str, EntitySchema]:
    topic_schemas: dict[str, EntitySchema] = {}
    for entity in entities:
        schema = entity_schemas.get(entity)
        topic = topic_maps.main.get(entity)
        if topic is None or schema is None:
            continue
        topic_schemas[topic] = schema
    return topic_schemas


def build_topic_schemas(
    topic_maps,
    entity_schemas: Mapping[str, EntitySchema],
    entities: list[str],
) -> dict[str, EntitySchema]:
    topic_schemas = build_entity_topic_schemas(topic_maps, entity_schemas, entities)
    topic_schemas.update(build_system_topic_schemas(topic_maps))
    return topic_schemas


DLQ_SCHEMA = EntitySchema(
    entity="dlq",
    message_name="UnifiedDlqRecord",
    fields=(
        FieldSchema("chain", "string"),
        FieldSchema("network", "string"),
        FieldSchema("pipeline", "string"),
        FieldSchema("entity", "string"),
        FieldSchema("cursor", "int64"),
        FieldSchema("stage", "string"),
        FieldSchema("error_type", "string"),
        FieldSchema("error_message", "string"),
        FieldSchema("payload", "string"),
        FieldSchema("context", "string"),
        FieldSchema("retry_count", "int64"),
        FieldSchema("max_retry", "int64"),
        FieldSchema("status", "string"),
        FieldSchema("first_seen_at", "int64"),
        FieldSchema("last_attempt_at", "int64"),
        FieldSchema("next_retry_at", "int64"),
        FieldSchema("id", "string"),
        FieldSchema("ingest_timestamp", "int64"),
    ),
)


CHECKPOINT_SCHEMA = EntitySchema(
    entity="checkpoint",
    message_name="UnifiedCheckpointRecord",
    fields=(
        FieldSchema("cursor", "int64"),
        FieldSchema("status", "string"),
        FieldSchema("updated_at_ms", "int64"),
        FieldSchema("pipeline", "string"),
        FieldSchema("chain_uid", "string"),
        FieldSchema("chain_type", "string"),
        FieldSchema("network", "string"),
        FieldSchema("mode", "string"),
        FieldSchema("primary_unit", "string"),
        FieldSchema("entities", "string", repeated=True),
        FieldSchema("error", "string"),
        FieldSchema("id", "string"),
        FieldSchema("ingest_timestamp", "int64"),
    ),
)


WATERMARK_STATE_SCHEMA = EntitySchema(
    entity="watermark_state",
    message_name="UnifiedWatermarkStateRecord",
    fields=(
        FieldSchema("cursor", "int64"),
        FieldSchema("status", "string"),
        FieldSchema("updated_at_ms", "int64"),
        FieldSchema("pipeline", "string"),
        FieldSchema("chain_uid", "string"),
        FieldSchema("chain_type", "string"),
        FieldSchema("network", "string"),
        FieldSchema("mode", "string"),
        FieldSchema("primary_unit", "string"),
        FieldSchema("entities", "string", repeated=True),
        FieldSchema("error", "string"),
        FieldSchema("id", "string"),
        FieldSchema("ingest_timestamp", "int64"),
    ),
)
