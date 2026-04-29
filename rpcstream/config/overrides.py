from __future__ import annotations

from typing import Sequence

from rpcstream.config.naming import build_pipeline_name
from rpcstream.config.schema import PipelineConfig


def apply_runtime_overrides(
    cfg: PipelineConfig,
    *,
    mode: str | None = None,
    from_value: str | int | None = None,
    to_value: int | None = None,
    entities: Sequence[str] | None = None,
    eos_enabled: bool | None = None,
) -> PipelineConfig:
    raw = cfg.model_dump(by_alias=True)
    pipeline = dict(raw.get("pipeline") or {})
    kafka = dict(raw.get("kafka") or {})
    eos = dict((kafka.get("eos") or {}))
    changed = False

    if mode is not None:
        pipeline["mode"] = mode
        changed = True

    if from_value is not None:
        pipeline["from"] = from_value
        changed = True

    if to_value is not None:
        pipeline["to"] = to_value
        changed = True

    if mode is None:
        pipeline["mode"] = "backfill" if pipeline.get("to") is not None else "realtime"
    normalized_mode = str(pipeline.get("mode") or "").strip().lower()
    if normalized_mode == "realtime":
        pipeline.pop("to", None)

    if entities is not None:
        raw["entities"] = list(entities)
        changed = True

    if eos_enabled is not None:
        eos["enabled"] = eos_enabled
        kafka["eos"] = eos
        raw["kafka"] = kafka
        changed = True

    if changed:
        pipeline["name"] = build_pipeline_name(
            chain_name=str(raw["chain"]["name"]),
            network=str(raw["chain"]["network"]),
            mode=str(pipeline.get("mode") or ""),
            from_value=pipeline.get("from"),
            to_value=pipeline.get("to"),
        )
        raw["pipeline"] = pipeline

    return PipelineConfig(**raw)
