"""
Structured JSON logging for all ETL microservices.

Usage:
    from common.logging_config import configure_service_logging
    logger = configure_service_logging('clean-nan-service')
"""

import json
import logging
import sys
from datetime import datetime, timezone


class JSONFormatter(logging.Formatter):
    """Formats log records as single-line JSON for machine-parseable output."""

    def format(self, record):
        log_entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "service": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }
        # Merge extra fields injected via CorrelationAdapter or direct assignment
        if hasattr(record, "correlation_id"):
            log_entry["correlation_id"] = record.correlation_id
        if hasattr(record, "dataset_name"):
            log_entry["dataset_name"] = record.dataset_name
        if hasattr(record, "extra_fields") and isinstance(record.extra_fields, dict):
            log_entry.update(record.extra_fields)
        if record.exc_info and not record.exc_text:
            record.exc_text = self.formatException(record.exc_info)
        if record.exc_text:
            log_entry["exception"] = record.exc_text
        return json.dumps(log_entry, default=str)


class CorrelationAdapter(logging.LoggerAdapter):
    """Logger adapter that injects correlation_id and dataset_name into every log record."""

    def process(self, msg, kwargs):
        extra = kwargs.get("extra", {})
        extra.setdefault("correlation_id", self.extra.get("correlation_id", "-"))
        extra.setdefault("dataset_name", self.extra.get("dataset_name", "-"))
        kwargs["extra"] = extra
        return msg, kwargs


def configure_service_logging(service_name, level=logging.INFO):
    """
    Configure structured JSON logging for a microservice.

    Returns a standard logging.Logger (not an adapter).
    Use get_correlation_logger() at request time for correlation context.
    """
    logger = logging.getLogger(service_name)
    # Prevent duplicate handlers on repeated calls
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JSONFormatter())
        logger.setLevel(level)
        logger.addHandler(handler)
        logger.propagate = False
    return logger


def get_correlation_logger(service_name, correlation_id=None, dataset_name=None):
    """
    Return a CorrelationAdapter wrapping the service logger,
    with correlation_id and dataset_name injected into every log line.
    """
    logger = logging.getLogger(service_name)
    return CorrelationAdapter(logger, {
        "correlation_id": correlation_id or "-",
        "dataset_name": dataset_name or "-",
    })
