import sys
import logging
import json
from datetime import datetime, timezone

# -----------------------------
# JSON formatter (Loki / OpenSearch)
# -----------------------------
class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        log = {
            "ts": datetime.now(timezone.utc)
            .isoformat(timespec="milliseconds")
            .replace("+00:00", "Z"),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key.startswith("_") or key in (
                "name","msg","args","levelname","levelno","pathname","filename",
                "module","exc_info","exc_text","stack_info","lineno","funcName",
                "created","msecs","relativeCreated","thread","threadName",
                "processName","process"
            ):
                continue
            log[key] = value
        return json.dumps(log, ensure_ascii=False)

def setup_logging():
    logger = logging.getLogger("web3_ingestion")
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    if not logger.handlers:
        logger.addHandler(handler)
    logger.propagate = False
    return logger

log = setup_logging()