import time

from rpcstream.utils.logger import JsonLogger


class ThrottledLogger:
    """Wraps a JsonLogger and rate-limits repetitive log lines by (level,
    message-template). During a failure storm the same error template can fire
    thousands of times per second; this caps each template to ``max_per_sec``
    lines and periodically emits a summary, so disk I/O no longer spikes.

    Only the structured ``message`` string is used as the template key, which
    is exactly the fixed template used throughout the codebase (volatile data
    lives in the kwargs, not the message).
    """

    def __init__(self, wrapped: JsonLogger, max_per_sec: float = 5.0, summary_interval_sec: float = 10.0):
        self._wrapped = wrapped
        self._min_spacing = 1.0 / max(0.1, max_per_sec)
        self._summary_interval = summary_interval_sec
        # key=(level, message) -> [last_emit_ts, suppressed_count, last_summary_ts]
        self._buckets: dict = {}

    def _check(self, level: str, message: str) -> bool:
        key = (level, message)
        now = time.monotonic()
        bucket = self._buckets.get(key)
        if bucket is None:
            self._buckets[key] = [now, 0, now]
            return True

        last_emit, suppressed, last_summary = bucket
        if suppressed > 0 and now - last_summary >= self._summary_interval:
            # Bypass throttling for the summary line itself.
            self._wrapped._log(
                "warn",
                "log.throttled_summary",
                message=message,
                level=level,
                suppressed=suppressed,
            )
            bucket[1] = 0
            bucket[2] = now

        if now - last_emit >= self._min_spacing:
            bucket[0] = now
            return True

        bucket[1] += 1
        return False

    def debug(self, message, **kwargs):
        if self._check("debug", message):
            self._wrapped.debug(message, **kwargs)

    def info(self, message, **kwargs):
        if self._check("info", message):
            self._wrapped.info(message, **kwargs)

    def warn(self, message, **kwargs):
        if self._check("warn", message):
            self._wrapped.warn(message, **kwargs)

    def error(self, message, **kwargs):
        if self._check("error", message):
            self._wrapped.error(message, **kwargs)

    def __getattr__(self, name):
        # Delegate anything not overridden (isEnabledFor, level, _otel_logger, ...).
        return getattr(self._wrapped, name)
