from __future__ import annotations

import asyncio
import os

import typer
import yaml

from rpcstream.config.loader import load_pipeline_config
from rpcstream.config.overrides import apply_runtime_overrides

def default_config_path() -> str:
    return os.getenv("PIPELINE_CONFIG", "pipeline.yaml")


def load_effective_config(
    *,
    config_path: str,
    mode: str | None = None,
    from_value: str | int | None = None,
    to_value: int | None = None,
    entities: list[str] | None = None,
    eos_enabled: bool | None = None,
):
    config = load_pipeline_config(config_path)
    if (
        mode is None
        and from_value is None
        and to_value is None
        and entities is None
        and eos_enabled is None
    ):
        return config
    return apply_runtime_overrides(
        config,
        mode=mode,
        from_value=from_value,
        to_value=to_value,
        entities=entities,
        eos_enabled=eos_enabled,
    )


def dump_config(config) -> str:
    return yaml.safe_dump(
        config.model_dump(by_alias=True),
        sort_keys=False,
        allow_unicode=False,
    )


def parse_entities(values: list[str] | None) -> list[str] | None:
    if not values:
        return None

    entities: list[str] = []
    seen: set[str] = set()
    for value in values:
        for part in str(value).split(","):
            entity = part.strip()
            if not entity or entity in seen:
                continue
            seen.add(entity)
            entities.append(entity)
    return entities or None


def infer_ingest_mode(*, from_value: str | int | None, to_value: int | None) -> str:
    if from_value is None and to_value is None:
        return "realtime"
    if from_value is not None and to_value is None:
        return "realtime"
    if from_value is not None and to_value is not None:
        return "backfill"
    raise ValueError("--to requires --from")


def run_async(coro) -> None:
    try:
        asyncio.run(coro)
    except KeyboardInterrupt:
        raise typer.Exit(130) from None


def fail(exc: Exception) -> None:
    typer.secho(str(exc), fg=typer.colors.RED, err=True)
    raise typer.Exit(1) from exc
