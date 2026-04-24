from __future__ import annotations

import hashlib
import json
import time
from typing import Any

PENDING = "pending"
RETRYING = "retrying"
FAILED = "failed"
RESOLVED = "resolved"

RETRYABLE_STATUSES = {PENDING, RETRYING}


def build_unified_dlq_record(
    *,
    chain: str,
    network: str,
    pipeline: str,
    entity: str,
    block_number: int | None,
    stage: str,
    error_type: str,
    error_message: str,
    payload: Any = None,
    context: dict[str, Any] | None = None,
    retry_count: int = 0,
    max_retry: int = 0,
    status: str = "pending",
    first_seen_at: int | None = None,
    last_attempt_at: int | None = None,
    next_retry_at: int | None = None,
    ingest_timestamp: int | None = None,
    record_id: str | None = None,
) -> dict[str, Any]:
    now_ms = int(time.time() * 1000)
    first_seen = first_seen_at if first_seen_at is not None else now_ms
    last_attempt = last_attempt_at if last_attempt_at is not None else now_ms
    ingest_ts = ingest_timestamp if ingest_timestamp is not None else now_ms

    record = {
        "chain": chain,
        "network": network,
        "pipeline": pipeline,
        "entity": entity,
        "block_number": block_number,
        "stage": stage,
        "error_type": error_type,
        "error_message": error_message,
        "payload": payload if payload is not None else {},
        "context": context or {},
        "retry_count": retry_count,
        "max_retry": max_retry,
        "status": status,
        "first_seen_at": first_seen,
        "last_attempt_at": last_attempt,
        "next_retry_at": next_retry_at,
        "ingest_timestamp": ingest_ts,
    }

    partition_block = block_number if block_number is not None else "unknown"
    record["kafka_partition_key"] = f"{chain}_{entity}_{partition_block}"
    record["id"] = record_id or _build_record_id(record)
    return record


def compute_next_retry_at(
    *,
    retry_count: int,
    base_delay_ms: int = 1000,
    now_ms: int | None = None,
) -> int:
    current_time = now_ms if now_ms is not None else int(time.time() * 1000)
    multiplier = 2 ** max(retry_count - 1, 0)
    return current_time + (multiplier * base_delay_ms)


def should_retry_record(record: dict[str, Any], now_ms: int | None = None) -> bool:
    if record.get("status") not in RETRYABLE_STATUSES:
        return False

    retry_count = int(record.get("retry_count") or 0)
    max_retry = int(record.get("max_retry") or 0)
    if retry_count >= max_retry:
        return False

    due_at = record.get("next_retry_at")
    if due_at in (None, 0):
        return True

    current_time = now_ms if now_ms is not None else int(time.time() * 1000)
    return current_time >= int(due_at)


def retry_delay_ms(record: dict[str, Any], now_ms: int | None = None) -> int:
    due_at = record.get("next_retry_at")
    if due_at in (None, 0):
        return 0
    current_time = now_ms if now_ms is not None else int(time.time() * 1000)
    return max(int(due_at) - current_time, 0)


def build_retry_record(
    previous_record: dict[str, Any],
    *,
    error_type: str,
    error_message: str,
    payload: Any = None,
    context: dict[str, Any] | None = None,
    base_delay_ms: int = 1000,
    now_ms: int | None = None,
) -> dict[str, Any]:
    current_time = now_ms if now_ms is not None else int(time.time() * 1000)
    next_retry_count = int(previous_record.get("retry_count") or 0) + 1
    max_retry = int(previous_record.get("max_retry") or 0)
    exhausted = next_retry_count >= max_retry

    return build_unified_dlq_record(
        chain=previous_record["chain"],
        network=previous_record["network"],
        pipeline=previous_record["pipeline"],
        entity=previous_record["entity"],
        block_number=previous_record.get("block_number"),
        stage=previous_record["stage"],
        error_type=error_type,
        error_message=error_message,
        payload=payload if payload is not None else previous_record.get("payload"),
        context=context if context is not None else previous_record.get("context"),
        retry_count=next_retry_count,
        max_retry=max_retry,
        status=FAILED if exhausted else RETRYING,
        first_seen_at=_normalize_optional_int(previous_record.get("first_seen_at")),
        last_attempt_at=current_time,
        next_retry_at=None
        if exhausted
        else compute_next_retry_at(
            retry_count=next_retry_count,
            base_delay_ms=base_delay_ms,
            now_ms=current_time,
        ),
        ingest_timestamp=current_time,
        record_id=previous_record.get("id"),
    )


def build_resolved_record(
    previous_record: dict[str, Any],
    *,
    now_ms: int | None = None,
) -> dict[str, Any]:
    current_time = now_ms if now_ms is not None else int(time.time() * 1000)
    return build_unified_dlq_record(
        chain=previous_record["chain"],
        network=previous_record["network"],
        pipeline=previous_record["pipeline"],
        entity=previous_record["entity"],
        block_number=previous_record.get("block_number"),
        stage=previous_record["stage"],
        error_type=previous_record.get("error_type", "Resolved"),
        error_message=previous_record.get("error_message", "resolved"),
        payload=previous_record.get("payload"),
        context=previous_record.get("context"),
        retry_count=int(previous_record.get("retry_count") or 0),
        max_retry=int(previous_record.get("max_retry") or 0),
        status=RESOLVED,
        first_seen_at=_normalize_optional_int(previous_record.get("first_seen_at")),
        last_attempt_at=current_time,
        next_retry_at=None,
        ingest_timestamp=current_time,
        record_id=previous_record.get("id"),
    )


def matches_replay_filter(
    record: dict[str, Any],
    *,
    entity: str | None = None,
    status: str | None = None,
    stage: str | None = None,
    pipeline: str | None = None,
    chain: str | None = None,
) -> bool:
    checks = (
        ("entity", entity),
        ("status", status),
        ("stage", stage),
        ("pipeline", pipeline),
        ("chain", chain),
    )
    for field, expected in checks:
        if expected is None:
            continue
        if record.get(field) != expected:
            return False
    return True


def _build_record_id(record: dict[str, Any]) -> str:
    digest = hashlib.sha256(
        json.dumps(
            {
                "chain": record["chain"],
                "network": record["network"],
                "pipeline": record["pipeline"],
                "entity": record["entity"],
                "block_number": record["block_number"],
                "stage": record["stage"],
                "error_type": record["error_type"],
                "error_message": record["error_message"],
                "first_seen_at": record["first_seen_at"],
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    return digest[:32]


def _normalize_optional_int(value: Any) -> int | None:
    if value in (None, 0, "0", ""):
        return None
    return int(value)
