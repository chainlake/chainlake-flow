# load YAML + env

import os
import re
from pathlib import Path

import yaml

from .naming import build_pipeline_name
from .schema import PipelineConfig
from .profiles.store import default_chain_profiles_path, resolve_chain_config


ENV_LINE_RE = re.compile(r"^(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)=(.*)$")


def load_pipeline_config(path: str) -> PipelineConfig:
    _load_env_file(path)
    with open(path, "r") as f:
        raw = yaml.safe_load(f) or {}

    profiles_path = os.getenv("CHAIN_PROFILES_PATH")
    if not profiles_path:
        profiles_path = str(default_chain_profiles_path())

    raw["chain"] = resolve_chain_config(raw.get("chain", {}), profiles_path=profiles_path)
    raw["pipeline"] = _fill_pipeline_name(raw.get("pipeline", {}), raw["chain"])
    return PipelineConfig(**raw)


def _load_env_file(config_path: str) -> None:
    env_path = _find_env_path(config_path)
    if env_path is None or not env_path.exists():
        return

    for raw_line in env_path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue

        match = ENV_LINE_RE.match(line)
        if not match:
            continue

        key, value = match.groups()
        cleaned = _clean_env_value(value)
        os.environ.setdefault(key, cleaned)


def _find_env_path(config_path: str) -> Path | None:
    config_dir = Path(config_path).resolve().parent

    for current in (config_dir, *config_dir.parents):
        candidate = current / ".env"
        if candidate.exists():
            return candidate

    cwd_candidate = Path.cwd() / ".env"
    if cwd_candidate.exists():
        return cwd_candidate
    return None


def _clean_env_value(value: str) -> str:
    cleaned = value.strip()
    if len(cleaned) >= 2 and cleaned[0] == cleaned[-1] and cleaned[0] in {"'", '"'}:
        return cleaned[1:-1]
    return cleaned


def _fill_pipeline_name(pipeline_cfg: dict, chain_cfg: dict) -> dict:
    pipeline = dict(pipeline_cfg or {})
    name = pipeline.get("name")
    if name:
        return pipeline
    mode = pipeline.get("mode")
    if mode is None:
        mode = "backfill" if pipeline.get("to") is not None else "realtime"

    pipeline["name"] = build_pipeline_name(
        chain_name=str(chain_cfg.get("name") or ""),
        network=str(chain_cfg.get("network") or ""),
        mode=str(mode or ""),
        from_value=pipeline.get("from"),
        to_value=pipeline.get("to"),
    )
    return pipeline
