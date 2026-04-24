from dataclasses import dataclass

UNIFIED_DLQ_TOPIC = "dlq.ingestion"


@dataclass
class TopicSet:
    main: str


@dataclass
class TopicMaps:
    main: dict[str, str]
    dlq: str


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
