"""Unit tests for common/logging_config.py — structured JSON logging."""

import json
import logging
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "services"))
from common.logging_config import CorrelationAdapter, JSONFormatter, configure_service_logging, get_correlation_logger


class TestJSONFormatter:
    def test_formats_as_valid_json(self):
        formatter = JSONFormatter()
        record = logging.LogRecord(
            name="test-service", level=logging.INFO, pathname="test.py",
            lineno=10, msg="hello world", args=(), exc_info=None,
        )
        output = formatter.format(record)
        parsed = json.loads(output)
        assert parsed["message"] == "hello world"
        assert parsed["level"] == "INFO"
        assert parsed["service"] == "test-service"
        assert parsed["line"] == 10
        assert "timestamp" in parsed

    def test_includes_correlation_id_when_present(self):
        formatter = JSONFormatter()
        record = logging.LogRecord(
            name="svc", level=logging.INFO, pathname="x.py",
            lineno=1, msg="msg", args=(), exc_info=None,
        )
        record.correlation_id = "abc-123"
        parsed = json.loads(formatter.format(record))
        assert parsed["correlation_id"] == "abc-123"

    def test_includes_dataset_name_when_present(self):
        formatter = JSONFormatter()
        record = logging.LogRecord(
            name="svc", level=logging.INFO, pathname="x.py",
            lineno=1, msg="msg", args=(), exc_info=None,
        )
        record.dataset_name = "my_dataset"
        parsed = json.loads(formatter.format(record))
        assert parsed["dataset_name"] == "my_dataset"

    def test_includes_exception_info(self):
        formatter = JSONFormatter()
        try:
            raise ValueError("test error")
        except ValueError:
            import sys
            exc_info = sys.exc_info()

        record = logging.LogRecord(
            name="svc", level=logging.ERROR, pathname="x.py",
            lineno=1, msg="failed", args=(), exc_info=exc_info,
        )
        parsed = json.loads(formatter.format(record))
        assert "exception" in parsed
        assert "ValueError: test error" in parsed["exception"]

    def test_no_correlation_id_when_absent(self):
        formatter = JSONFormatter()
        record = logging.LogRecord(
            name="svc", level=logging.INFO, pathname="x.py",
            lineno=1, msg="msg", args=(), exc_info=None,
        )
        parsed = json.loads(formatter.format(record))
        assert "correlation_id" not in parsed


class TestCorrelationAdapter:
    def test_injects_correlation_id(self):
        logger = logging.getLogger("test.adapter.inject")
        adapter = CorrelationAdapter(logger, {"correlation_id": "xyz", "dataset_name": "ds1"})
        msg, kwargs = adapter.process("test", {})
        assert kwargs["extra"]["correlation_id"] == "xyz"
        assert kwargs["extra"]["dataset_name"] == "ds1"

    def test_defaults_to_dash(self):
        logger = logging.getLogger("test.adapter.default")
        adapter = CorrelationAdapter(logger, {})
        msg, kwargs = adapter.process("test", {})
        assert kwargs["extra"]["correlation_id"] == "-"
        assert kwargs["extra"]["dataset_name"] == "-"

    def test_does_not_override_existing_extra(self):
        logger = logging.getLogger("test.adapter.existing")
        adapter = CorrelationAdapter(logger, {"correlation_id": "abc", "dataset_name": "ds"})
        msg, kwargs = adapter.process("test", {"extra": {"correlation_id": "existing"}})
        assert kwargs["extra"]["correlation_id"] == "existing"


class TestConfigureServiceLogging:
    def test_returns_logger(self):
        logger = configure_service_logging("test-configure-svc")
        assert isinstance(logger, logging.Logger)
        assert logger.name == "test-configure-svc"

    def test_has_json_formatter(self):
        logger = configure_service_logging("test-json-fmt-svc")
        assert len(logger.handlers) >= 1
        assert isinstance(logger.handlers[0].formatter, JSONFormatter)

    def test_no_duplicate_handlers(self):
        name = "test-no-dup-svc"
        configure_service_logging(name)
        handler_count = len(logging.getLogger(name).handlers)
        configure_service_logging(name)
        assert len(logging.getLogger(name).handlers) == handler_count

    def test_propagate_disabled(self):
        logger = configure_service_logging("test-propagate-svc")
        assert logger.propagate is False


class TestGetCorrelationLogger:
    def test_returns_adapter(self):
        adapter = get_correlation_logger("test-corr-svc", "id-1", "ds-1")
        assert isinstance(adapter, CorrelationAdapter)
        assert adapter.extra["correlation_id"] == "id-1"
        assert adapter.extra["dataset_name"] == "ds-1"

    def test_defaults_to_dash_when_none(self):
        adapter = get_correlation_logger("test-corr-none-svc")
        assert adapter.extra["correlation_id"] == "-"
        assert adapter.extra["dataset_name"] == "-"
