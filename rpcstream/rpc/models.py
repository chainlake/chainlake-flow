from dataclasses import dataclass, field
from typing import Any, Optional

@dataclass
class RpcTaskMeta:
    task_id: int
    submit_ts: float
    extra: dict[str, Any]

@dataclass
class RpcErrorResult:
    error: Exception
    meta: RpcTaskMeta

@dataclass
class ClientMetrics:
    request_total: int = 0
    request_success: int = 0
    request_error: int = 0
    retry_total: int = 0
    timeout_total: int = 0
    transport_error_total: int = 0
    rpc_error_total: int = 0
    inflight: int = 0
    latency_ema_ms: Optional[float] = None
