# src/metrics/runtime.py
from contextvars import ContextVar
from typing import Optional
from .context import MetricsContext

# Task-local metrics context
# Async-safe glue
_current_metrics: ContextVar[Optional[MetricsContext]] = ContextVar(
    "current_metrics",
    default=None,
)


def set_current_metrics(ctx: MetricsContext):
    """
    Bind metrics context to current asyncio task.
    """
    _current_metrics.set(ctx)


def get_metrics() -> MetricsContext:
    """
    Get metrics context bound to current asyncio task.
    """
    ctx = _current_metrics.get()
    if ctx is None:
        raise RuntimeError(
            "MetricsContext is not set for current task. "
            "Call set_current_metrics() at task entry."
        )
    return ctx

