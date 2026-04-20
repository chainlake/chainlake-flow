import json
import time
import logging
from opentelemetry import trace

def _get_trace_id():
    span = trace.get_current_span()
    ctx = span.get_span_context()
    if ctx.trace_id == 0:
        return None
    return format(ctx.trace_id, "032x")

class JsonLogger:
    
    LEVELS = {
        "debug": 10,
        "info": 20,
        "warn": 30,
        "error": 40,
    }
    
    def __init__(self, name="rpcstream", level="INFO"):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level.upper())
        self.level = level.lower()

        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter('%(message)s'))
        self.logger.handlers = [handler]

    def isEnabledFor(self, level_num):
        return level_num >= self.LEVELS[self.level]

    def _log(self, level, message, **kwargs):
        trace_id = _get_trace_id()
        
        log = {
            "level": level,
            "time": int(time.time() * 1000),
            "message": message,
            **kwargs
        }

        if trace_id:
            log["trace_id"] = trace_id

        self.logger.log(getattr(logging, level.upper()), json.dumps(log))

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