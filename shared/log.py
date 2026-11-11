"""Structured JSON logging on top of stdlib ``logging``.

Usage::

    log = get_logger("nlp_worker")
    log.info("batch processed", extra={"post_id": pid, "n": 32})

Every record is emitted as one JSON line containing ``service`` and any ``extra`` fields.
"""

from __future__ import annotations

import json
import logging
import sys
import time
from typing import Any

_RESERVED = set(vars(logging.makeLogRecord({}))) | {"message", "asctime", "taskName"}
_configured = False


class JsonFormatter(logging.Formatter):
    def __init__(self, service: str) -> None:
        super().__init__()
        self.service = service

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": round(record.created, 3),
            "level": record.levelname.lower(),
            "service": self.service,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key not in _RESERVED and not key.startswith("_"):
                payload[key] = value
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def setup_logging(service: str, level: str = "INFO") -> None:
    global _configured
    root = logging.getLogger()
    for h in list(root.handlers):
        root.removeHandler(h)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter(service))
    root.addHandler(handler)
    root.setLevel(level.upper())
    # aiokafka is chatty at INFO
    logging.getLogger("aiokafka").setLevel(logging.WARNING)
    logging.Formatter.converter = time.gmtime
    _configured = True


def get_logger(service: str, level: str | None = None) -> logging.Logger:
    if not _configured:
        from shared.config import get_settings

        setup_logging(service, level or get_settings().log_level)
    return logging.getLogger(service)
