from dataclasses import dataclass, field
from typing import Any

@dataclass
class RpcTaskMeta:
    task_id: int
    submit_ts: float
    extra: dict[str, Any]

@dataclass
class RpcErrorResult:
    error: str
    meta: RpcTaskMeta
    details: dict[str, Any] = field(default_factory=dict)
    expected: bool = False


@dataclass
class RpcResponseError(Exception):
    method: str
    code: Any = None
    message: str = "json-rpc request failed"
    data: dict[str, Any] = field(default_factory=dict)
    request_meta: dict[str, Any] = field(default_factory=dict)
    raw_error: Any = None

    def __post_init__(self):
        super().__init__(self.summary())

    @classmethod
    def from_payload(
        cls,
        method: str,
        error: Any,
        request_meta: dict[str, Any] | None = None,
    ) -> "RpcResponseError":
        if isinstance(error, dict):
            return cls(
                method=method,
                code=error.get("code"),
                message=error.get("message", "json-rpc request failed"),
                data=error.get("data") or {},
                request_meta=request_meta or {},
                raw_error=error,
            )

        return cls(
            method=method,
            message=str(error),
            request_meta=request_meta or {},
            raw_error=error,
        )

    def summary(self) -> str:
        parts = [f"method={self.method}"]
        if self.code is not None:
            parts.append(f"code={self.code}")
        parts.append(f"message={self.message}")
        cursor = self.requested_cursor()
        if cursor is not None:
            parts.append(f"cursor={cursor}")
        return "rpc_response_error(" + ", ".join(parts) + ")"

    def requested_cursor(self) -> Any:
        return self.request_meta.get("cursor")

    def _cause_list(self) -> list[dict[str, Any]]:
        cause = self.data.get("cause")
        if isinstance(cause, list):
            return [item for item in cause if isinstance(item, dict)]
        return []

    def _upstream_block_unavailable_causes(self) -> list[dict[str, Any]]:
        matches = []
        for cause in self._cause_list():
            if cause.get("code") == "ErrUpstreamBlockUnavailable":
                matches.append(cause)
                continue

            nested = cause.get("cause")
            if isinstance(nested, dict) and nested.get("code") == "ErrUpstreamBlockUnavailable":
                matches.append(nested)

        return matches

    def is_upstream_block_not_ready(self) -> bool:
        if "upstream does not have the requested block yet" in str(self.message).lower():
            return True

        return bool(self._upstream_block_unavailable_causes())

    def is_expected_warning(self) -> bool:
        return self.is_upstream_block_not_ready()

    def log_fields(self) -> dict[str, Any]:
        fields = {
            "rpc_error_code": self.code,
            "rpc_error_message": self.message,
        }

        requested_cursor = self.requested_cursor()
        if requested_cursor is not None:
            fields["cursor"] = requested_cursor

        if isinstance(self.data, dict):
            details = self.data.get("details") or {}
            if isinstance(details, dict):
                for src_key, dst_key in (
                    ("networkId", "network_id"),
                    ("projectId", "project_id"),
                    ("upstreams", "upstreams_total"),
                    ("durationMs", "duration_ms"),
                    ("retries", "retries"),
                    ("attempts", "attempts"),
                    ("hedges", "hedges"),
                ):
                    value = details.get(src_key)
                    if value is not None:
                        fields[dst_key] = value

        if self.is_upstream_block_not_ready():
            block_causes = self._upstream_block_unavailable_causes()
            latest_blocks = [
                item.get("details", {}).get("latestBlock")
                for item in block_causes
                if isinstance(item.get("details"), dict)
                and item.get("details", {}).get("latestBlock") is not None
            ]
            finalized_blocks = [
                item.get("details", {}).get("finalizedBlock")
                for item in block_causes
                if isinstance(item.get("details"), dict)
                and item.get("details", {}).get("finalizedBlock") is not None
            ]
            if block_causes:
                fields["not_ready_upstreams"] = len(block_causes)
            if latest_blocks:
                fields["min_latest_cursor"] = min(latest_blocks)
                fields["max_latest_cursor"] = max(latest_blocks)
            if finalized_blocks:
                fields["min_finalized_cursor"] = min(finalized_blocks)
                fields["max_finalized_cursor"] = max(finalized_blocks)

        return {key: value for key, value in fields.items() if value is not None}

    def __str__(self) -> str:
        return self.summary()


def summarize_exception(exc: Exception) -> str:
    if isinstance(exc, RpcResponseError):
        return exc.summary()
    return repr(exc)


def exception_log_fields(exc: Exception) -> dict[str, Any]:
    if isinstance(exc, RpcResponseError):
        return exc.log_fields()
    return {"error": str(exc)}


def is_expected_rpc_warning(exc: Exception) -> bool:
    return isinstance(exc, RpcResponseError) and exc.is_expected_warning()
