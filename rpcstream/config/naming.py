from __future__ import annotations

import re


_NON_ALNUM_RE = re.compile(r"[^a-z0-9]+")


def normalize_name_component(value: str) -> str:
    cleaned = value.strip().lower()
    cleaned = _NON_ALNUM_RE.sub("_", cleaned)
    cleaned = cleaned.strip("_")
    return cleaned or "unknown"


def build_pipeline_name(
    *,
    chain_name: str,
    network: str,
    mode: str,
    start_block,
    end_block=None,
    checkpoint_enabled: bool | None = None,
) -> str:
    parts = [
        normalize_name_component(chain_name),
        normalize_name_component(network),
        normalize_name_component(mode),
    ]

    normalized_mode = normalize_name_component(mode)
    if normalized_mode == "backfill":
        parts.append(_format_block_name(start_block))
        parts.append(_format_block_name(end_block))
        return "_".join(parts)

    start_name = _format_block_name(start_block)
    if checkpoint_enabled and start_name == "latest":
        start_name = "checkpointed_latest"
    parts.append(start_name)
    return "_".join(parts)


def _format_block_name(value) -> str:
    if value is None:
        return "unknown"
    if isinstance(value, int):
        return str(value)

    text = str(value).strip().lower()
    if not text:
        return "unknown"
    if text in {"latest", "checkpoint+1", "checkpoint_plus_1", "checkpointed_latest"}:
        return text.replace("+", "_")
    if text.isdigit():
        return text
    return normalize_name_component(text)
