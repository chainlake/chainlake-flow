from __future__ import annotations


SINK_ENTITY_ORDER = ("block", "transaction", "log", "token_transfer", "trace")
INTERNAL_ENTITY_ORDER = ("block", "transaction", "receipt", "log", "trace")

ENTITY_DEPENDENCIES = {
    "block": {"block"},
    "transaction": {"transaction", "receipt"},
    "log": {"block", "receipt", "log"},
    "token_transfer": {"log"},
    "trace": {"block", "trace"},
    "receipt": {"block", "receipt"},
}


def resolve_internal_entities(requested_entities: list[str]) -> list[str]:
    requested = {entity.strip().lower() for entity in requested_entities}
    internal = set()
    pending = list(requested)
    while pending:
        entity = pending.pop()
        if entity in internal:
            continue
        internal.add(entity)
        for dependency in ENTITY_DEPENDENCIES.get(entity, {entity}):
            if dependency not in internal:
                pending.append(dependency)
    return [entity for entity in INTERNAL_ENTITY_ORDER if entity in internal]


def resolve_sink_entities(requested_entities: list[str]) -> list[str]:
    requested = {entity.strip().lower() for entity in requested_entities}
    return [entity for entity in SINK_ENTITY_ORDER if entity in requested]


def topic_kind_for_entity(entity: str) -> str:
    if entity == "token_transfer":
        return ""
    if entity == "transaction":
        return "enriched"
    return "raw"
