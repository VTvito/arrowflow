"""Unit tests for common/service_utils.py — shared service utilities."""

import json
import os
import sys
import time

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "services"))

from common.service_utils import (
    create_service_counters,
    get_correlation_id,
    parse_x_params,
    save_metadata,
)

# We need a Flask app context to test request-aware helpers.
from flask import Flask


@pytest.fixture
def app():
    """Minimal Flask app for testing request context."""
    app = Flask(__name__)
    app.config["TESTING"] = True
    return app


# ── Prometheus counter tests ────────────────────────────────────────

class TestCreateServiceCounters:
    def test_returns_three_counters(self):
        req, suc, err = create_service_counters("test_svc_counters")
        assert req is not None
        assert suc is not None
        assert err is not None

    def test_counter_names(self):
        req, suc, err = create_service_counters("test_svc_names")
        assert "test_svc_names_requests" in req._name
        assert "test_svc_names_success" in suc._name
        assert "test_svc_names_error" in err._name


# ── get_correlation_id tests ────────────────────────────────────────

class TestGetCorrelationId:
    def test_reads_from_header(self, app):
        with app.test_request_context(headers={"X-Correlation-ID": "my-id-123"}):
            cid = get_correlation_id()
            assert cid == "my-id-123"

    def test_generates_uuid_when_absent(self, app):
        with app.test_request_context():
            cid = get_correlation_id()
            # Should be a valid UUID-like string
            assert len(cid) == 36
            assert cid.count("-") == 4

    def test_cached_in_flask_g(self, app):
        with app.test_request_context(headers={"X-Correlation-ID": "stable-id"}):
            first = get_correlation_id()
            second = get_correlation_id()
            assert first == second == "stable-id"


# ── parse_x_params tests ───────────────────────────────────────────

class TestParseXParams:
    def test_parses_valid_json(self, app):
        with app.test_request_context(headers={"X-Params": '{"key": "value"}'}):
            result = parse_x_params()
            assert result == {"key": "value"}

    def test_returns_empty_dict_when_absent(self, app):
        with app.test_request_context():
            result = parse_x_params()
            assert result == {}

    def test_raises_on_malformed_json(self, app):
        with app.test_request_context(headers={"X-Params": "{bad json}"}):
            with pytest.raises(ValueError, match="Malformed JSON"):
                parse_x_params()

    def test_nested_objects(self, app):
        params = {"rules": {"min_rows": 10, "check_duplicates": True}}
        with app.test_request_context(headers={"X-Params": json.dumps(params)}):
            result = parse_x_params()
            assert result == params


# ── save_metadata tests ────────────────────────────────────────────

class TestSaveMetadata:
    def test_writes_metadata_file(self, app, tmp_path, monkeypatch):
        # Point ensure_dataset_dirs to a temp directory
        data_dir = tmp_path / "data"
        metadata_dir = tmp_path / "metadata"
        data_dir.mkdir()
        metadata_dir.mkdir()

        import common.service_utils as su
        monkeypatch.setattr(su, "ensure_dataset_dirs", lambda ds: (str(data_dir), str(metadata_dir)))

        with app.test_request_context(headers={"X-Correlation-ID": "test-cid"}):
            start = time.time()
            path = save_metadata(
                "test-service", "test_dataset",
                extra_fields={"rows": 100, "columns": 5},
                start_time=start,
            )

        assert os.path.exists(path)
        with open(path) as f:
            meta = json.load(f)
        assert meta["service_name"] == "test-service"
        assert meta["dataset_name"] == "test_dataset"
        assert meta["correlation_id"] == "test-cid"
        assert meta["rows"] == 100
        assert "duration_sec" in meta
        assert "timestamp" in meta

    def test_metadata_without_start_time(self, app, tmp_path, monkeypatch):
        data_dir = tmp_path / "data"
        metadata_dir = tmp_path / "metadata"
        data_dir.mkdir()
        metadata_dir.mkdir()

        import common.service_utils as su
        monkeypatch.setattr(su, "ensure_dataset_dirs", lambda ds: (str(data_dir), str(metadata_dir)))

        with app.test_request_context():
            path = save_metadata("svc", "ds")

        with open(path) as f:
            meta = json.load(f)
        assert "duration_sec" not in meta
