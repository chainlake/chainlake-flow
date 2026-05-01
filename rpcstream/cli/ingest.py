from __future__ import annotations

from rpcstream.cli.common import (
    fail,
    infer_ingest_mode,
    load_effective_config,
    parse_entities,
    run_async,
)
from rpcstream.main import run_pipeline


def run_ingest(
    *,
    config_path: str,
    from_value: str | int | None,
    to_value: int | None,
    entity: list[str] | None,
) -> None:
    entities = parse_entities(entity)
    effective_from = "checkpoint" if from_value is None else from_value
    effective_to = to_value

    if infer_ingest_mode(from_value=from_value, to_value=to_value) == "backfill":
        normalized_from = str(effective_from).strip().lower()
        if normalized_from in {"checkpoint", "latest", "chainhead"}:
            raise ValueError("--from must be a numeric cursor when --to is provided")

    config = load_effective_config(
        config_path=config_path,
        from_value=effective_from,
        to_value=effective_to,
        entities=entities,
    )
    run_async(run_pipeline(config_path=config_path, config=config))


__all__ = [
    "run_ingest",
]
