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
    from_value,
    to_value=None,
) -> str:
    parts = [
        normalize_name_component(chain_name),
        normalize_name_component(network),
        normalize_name_component(mode),
    ]

    normalized_mode = normalize_name_component(mode)
    if normalized_mode == "backfill":
        parts.append(_format_block_name(from_value))
        parts.append(_format_block_name(to_value))
        return "_".join(parts)

    start_name = _format_block_name(from_value)
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
    if text == "latest":
        text = "chainhead"
    if text in {"chainhead", "checkpoint", "checkpoint+1", "checkpoint_plus_1", "checkpointed_latest"}:
        return text.replace("+", "_")
    if text.isdigit():
        return text
    return normalize_name_component(text)
