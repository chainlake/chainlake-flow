from __future__ import annotations

from rpcstream.cli.common import fail, load_effective_config, parse_entities, run_async
from rpcstream.derived_runtime import run_derived_pipeline


def run_ingest_derived(
    *,
    config_path: str,
    source_topic: str | None,
    from_block: int | None,
    to_block: int | None,
    entity: list[str] | None,
) -> None:
    entities = parse_entities(entity)
    config = load_effective_config(
        config_path=config_path,
        entities=entities,
    )
    run_async(
        run_derived_pipeline(
            config=config,
            source_topic=source_topic,
            from_block=from_block,
            to_block=to_block,
        )
    )


__all__ = ["run_ingest_derived"]
