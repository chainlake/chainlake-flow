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


def normalize_entity(entity: str) -> str:
    return entity.strip().lower()


def build_topics(cfg, entity: str) -> TopicSet:

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

    return TopicSet(
        main=render("raw"),
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

    return f"{cfg.chain.type}.{cfg.chain.name}.{cfg.chain.network}.checkpoint_cursor"
