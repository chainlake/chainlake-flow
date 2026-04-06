from datetime import datetime, timezone
from enum import Enum
# -----------------------------
# create current_utctime
# -----------------------------
def current_utctime():
    """Return the current UTC time string in ISO-8601 format with millisecond precision"""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def build_method_group_map(chain_cfg: dict) -> dict[str, str]:
    """
    eth_getLogs -> heavy
    eth_blockNumber -> light
    """
    mapping = {}
    for group, methods in chain_cfg.get("method_groups", {}).items():
        for m in methods:
            mapping[m] = group
    return mapping


def decide_resume_plan(
    *,
    resume_from_checkpoint: bool,
    last_state: dict | None,
    latest_block: int
):
    """
    Decide resume strategy for realtime ingestion.

    Returns:
        start_block: int
        job_name: str
        resume_mode: Literal["checkpoint", "chain_head"]
    """

    # 默认：从 chain head
    start_block = latest_block
    resume_mode = "chain_head"

    if not resume_from_checkpoint:
        return start_block, resume_mode

    if last_state and last_state.get("checkpoint") is not None:
        start_block = last_state["checkpoint"] + 1
        resume_mode = "checkpoint"

    return start_block, resume_mode