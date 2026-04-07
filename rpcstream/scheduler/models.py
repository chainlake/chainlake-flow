import json
from dataclasses import dataclass
from typing import Any


@dataclass(slots=True)
class SchedulerTask:
    """Scheduler payload persisted in Redis ordering queue / DLQ."""

    sequence: int
    method: str
    params: list[Any]
    retries: int = 0
    max_retries: int = 3
    metadata: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "sequence": self.sequence,
            "method": self.method,
            "params": self.params,
            "retries": self.retries,
            "max_retries": self.max_retries,
            "metadata": self.metadata or {},
        }

    @classmethod
    def from_raw(cls, raw: bytes | str) -> "SchedulerTask":
        payload = json.loads(raw.decode() if isinstance(raw, bytes) else raw)
        return cls(
            sequence=payload["sequence"],
            method=payload["method"],
            params=payload.get("params", []),
            retries=payload.get("retries", 0),
            max_retries=payload.get("max_retries", 3),
            metadata=payload.get("metadata", {}),
        )
