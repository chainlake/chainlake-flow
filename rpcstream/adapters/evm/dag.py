from __future__ import annotations


SINK_ENTITY_ORDER = ("block", "transaction", "log", "trace")
INTERNAL_ENTITY_ORDER = ("block", "transaction", "receipt", "log", "trace")

ENTITY_DEPENDENCIES = {
    "block": {"block"},
    "transaction": {"transaction", "receipt"},
    "log": {"block", "receipt", "log"},
    "trace": {"block", "trace"},
    "receipt": {"block", "receipt"},
}


def resolve_internal_entities(requested_entities: list[str]) -> list[str]:
    requested = {entity.strip().lower() for entity in requested_entities}
    internal = set()
    for entity in requested:
        internal.update(ENTITY_DEPENDENCIES.get(entity, {entity}))
    return [entity for entity in INTERNAL_ENTITY_ORDER if entity in internal]


def resolve_sink_entities(requested_entities: list[str]) -> list[str]:
    requested = {entity.strip().lower() for entity in requested_entities}
    return [entity for entity in SINK_ENTITY_ORDER if entity in requested]


def topic_kind_for_entity(entity: str) -> str:
    if entity == "transaction":
        return "enriched"
    return "raw"
