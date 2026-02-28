"""Unit tests for common/health.py — enriched health check response."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "services"))
from common.health import create_health_response


class TestCreateHealthResponse:
    def test_returns_dict(self):
        result = create_health_response("test-service")
        assert isinstance(result, dict)

    def test_status_ok(self):
        result = create_health_response("test-service")
        assert result["status"] == "ok"

    def test_service_name(self):
        result = create_health_response("my-svc")
        assert result["service"] == "my-svc"

    def test_version_from_env(self, monkeypatch):
        monkeypatch.setenv("SERVICE_VERSION", "2.5.0")
        result = create_health_response("svc")
        assert result["version"] == "2.5.0"

    def test_version_default(self, monkeypatch):
        monkeypatch.delenv("SERVICE_VERSION", raising=False)
        result = create_health_response("svc")
        assert result["version"] == "1.0.0"

    def test_uptime_is_positive(self):
        result = create_health_response("svc")
        assert result["uptime_seconds"] >= 0

    def test_timestamp_is_iso(self):
        result = create_health_response("svc")
        # ISO format contains 'T' separator
        assert "T" in result["timestamp"]

    def test_checks_structure(self):
        result = create_health_response("svc")
        assert "checks" in result
        assert "shared_volume" in result["checks"]
        assert "disk_space_mb" in result["checks"]

    def test_shared_volume_with_existing_dir(self, monkeypatch, tmp_path):
        monkeypatch.setenv("ETL_DATA_ROOT", str(tmp_path))
        result = create_health_response("svc")
        assert result["checks"]["shared_volume"] is True
        assert result["checks"]["disk_space_mb"] is not None
        assert result["checks"]["disk_space_mb"] > 0

    def test_shared_volume_with_missing_dir(self, monkeypatch):
        monkeypatch.setenv("ETL_DATA_ROOT", "/nonexistent/path/xyz123")
        result = create_health_response("svc")
        assert result["checks"]["shared_volume"] is False
        assert result["checks"]["disk_space_mb"] is None
