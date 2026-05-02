from dataclasses import dataclass

UNIFIED_DLQ_TOPIC = "dlq.ingestion"


@dataclass
class TopicSet:
    main: str


@dataclass
class TopicMaps:
    main: dict[str, str]
    dlq: str
    checkpoint: str
    watermark_state: str


def normalize_entity(entity: str) -> str:
    return entity.strip().lower()


def build_topics(cfg, entity: str, adapter=None) -> TopicSet:
    template = (
        cfg.kafka.common.topic_template
        or "{type}.{chain}.{network}.{kind}_{entity}"
    )

    def render(kind):
        return template.format(
            chain=cfg.chain.name,
            network=cfg.chain.network,
            type=cfg.chain.type,
            entity=entity,
            kind=kind,
        )

    topic_kind_for_entity = (
        adapter.topic_kind_for_entity
        if adapter is not None
        else (lambda _entity: "raw")
    )
    kind = topic_kind_for_entity(entity)
    if entity == "token_transfer" or not kind:
        return TopicSet(
            main=f"{cfg.chain.type}.{cfg.chain.name}.{cfg.chain.network}.{entity}",
        )

    return TopicSet(
        main=render(kind),
    )


def build_unified_dlq_topic(_cfg) -> str:
    return UNIFIED_DLQ_TOPIC


def build_checkpoint_topic(cfg) -> str:
    checkpoint = getattr(getattr(cfg, "pipeline", None), "checkpoint", None)
    pipeline_fields = getattr(getattr(cfg, "pipeline", None), "model_fields_set", set())
    root_fields = getattr(cfg, "model_fields_set", set())
    if "checkpoint" not in pipeline_fields and "checkpoint" in root_fields:
        checkpoint = getattr(cfg, "checkpoint", None)
    if checkpoint is None:
        checkpoint = getattr(cfg, "checkpoint", None)
    configured = getattr(checkpoint, "topic", None)
    if configured:
        return configured

    return f"{cfg.chain.type}.{cfg.chain.name}.{cfg.chain.network}.commit_watermark"


def build_watermark_state_topic(cfg) -> str:
    checkpoint_topic = build_checkpoint_topic(cfg)
    if checkpoint_topic.endswith(".commit_watermark"):
        return checkpoint_topic.removesuffix(".commit_watermark") + ".cursor_state"
    if checkpoint_topic.endswith(".checkpoint_cursor"):
        return checkpoint_topic.removesuffix(".checkpoint_cursor") + ".cursor_state"
    return f"{checkpoint_topic}.cursor_state"
