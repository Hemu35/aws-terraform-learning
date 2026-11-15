"""Structured JSON logging configuration.

All log records are emitted as single-line JSON objects so that log
aggregation systems (CloudWatch Logs Insights, Datadog, etc.) can parse
them without additional parsing rules.

Usage::

    from app.core.logging_config import setup_logging, get_logger

    setup_logging("INFO")
    logger = get_logger(__name__)
    logger.info("Job submitted", extra={"job_id": "abc-123"})
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone


class _JsonFormatter(logging.Formatter):
    """Formats log records as JSON lines.

    Output fields:
      - ``timestamp``: ISO-8601 UTC
      - ``level``: log level name
      - ``name``: logger name (usually ``__name__``)
      - ``message``: the formatted log message
      - Any extra key/value pairs passed via ``extra={}``
    """

    def format(self, record: logging.LogRecord) -> str:
        payload: dict = {
            "timestamp": datetime.fromtimestamp(
                record.created, tz=timezone.utc
            ).isoformat(),
            "level": record.levelname,
            "name": record.name,
            "message": record.getMessage(),
        }

        # Bubble up any structured extras (e.g. job_id, s3_key)
        for key, value in record.__dict__.items():
            if key not in {
                "args",
                "asctime",
                "created",
                "exc_info",
                "exc_text",
                "filename",
                "funcName",
                "levelname",
                "levelno",
                "lineno",
                "message",
                "module",
                "msecs",
                "msg",
                "name",
                "pathname",
                "process",
                "processName",
                "relativeCreated",
                "stack_info",
                "thread",
                "threadName",
                "taskName",
            }:
                payload[key] = value

        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)

        return json.dumps(payload, default=str)


def setup_logging(level: str = "INFO") -> None:
    """Configure the root logger to emit JSON to stdout.

    Call once at application startup (e.g. in ``app/main.py``).

    Args:
        level: Logging level string (``"DEBUG"``, ``"INFO"``, etc.).
    """
    numeric_level = getattr(logging, level.upper(), logging.INFO)

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(_JsonFormatter())

    root = logging.getLogger()
    root.setLevel(numeric_level)
    # Remove any handlers added by libraries before ours
    root.handlers.clear()
    root.addHandler(handler)


def get_logger(name: str) -> logging.Logger:
    """Return a named logger.

    Args:
        name: Typically ``__name__`` of the calling module.

    Returns:
        A :class:`logging.Logger` instance.
    """
    return logging.getLogger(name)
