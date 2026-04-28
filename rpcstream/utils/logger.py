import json
import time
import logging
from opentelemetry import trace
from opentelemetry._logs import LogRecord, SeverityNumber

def _get_trace_context():
    span = trace.get_current_span()
    ctx = span.get_span_context()
    if not ctx.is_valid:
        return None
    return {
        "trace_id": format(ctx.trace_id, "032x"),
        "span_id": format(ctx.span_id, "016x"),
    }

class JsonLogger:
    
    LEVELS = {
        "debug": 10,
        "info": 20,
        "warn": 30,
        "error": 40,
    }
    
    _OTEL_SEVERITY = {
        "debug": SeverityNumber.DEBUG,
        "info": SeverityNumber.INFO,
        "warn": SeverityNumber.WARN,
        "error": SeverityNumber.ERROR,
    }

    def __init__(self, name="rpcstream", level="INFO", logger_provider=None):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level.upper())
        self.level = level.lower()
        self._otel_logger = (
            logger_provider.get_logger(name)
            if logger_provider is not None
            else None
        )

        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter('%(message)s'))
        self.logger.handlers = [handler]
        self.logger.propagate = False

    def isEnabledFor(self, level_num):
        return level_num >= self.LEVELS[self.level]

    def _log(self, level, message, **kwargs):
        trace_context = _get_trace_context()
        
        log = {
            "level": level,
            "time": int(time.time() * 1000),
            "message": message,
            **kwargs
        }

        if trace_context:
            log.update(trace_context)

        self.logger.log(getattr(logging, level.upper()), json.dumps(log))
        self._emit_otel(level, message, log)

    def _emit_otel(self, level, message, log):
        if self._otel_logger is None:
            return

        span_context = trace.get_current_span().get_span_context()
        kwargs = {
            "timestamp": log["time"] * 1_000_000,
            "severity_text": level.upper(),
            "severity_number": self._OTEL_SEVERITY[level],
            "body": message,
            "attributes": {
                key: _otel_attribute_value(value)
                for key, value in log.items()
                if key != "message" and value is not None
            },
        }
        if span_context.is_valid:
            kwargs.update(
                trace_id=span_context.trace_id,
                span_id=span_context.span_id,
                trace_flags=span_context.trace_flags,
            )

        self._otel_logger.emit(LogRecord(**kwargs))

    def debug(self, message, **kwargs):
        if not self.logger.isEnabledFor(logging.DEBUG):
            return
        self._log("debug", message, **kwargs)

    def info(self, message, **kwargs):
        self._log("info", message, **kwargs)

    def warn(self, message, **kwargs):
        self._log("warn", message, **kwargs)

    def error(self, message, **kwargs):
        self._log("error", message, **kwargs)


def _otel_attribute_value(value):
    if isinstance(value, (str, bool, int, float)):
        return value
    if isinstance(value, (list, tuple)):
        if all(isinstance(item, (str, bool, int, float)) for item in value):
            return list(value)
        return json.dumps(value, separators=(",", ":"), default=str)
    return json.dumps(value, separators=(",", ":"), default=str)
