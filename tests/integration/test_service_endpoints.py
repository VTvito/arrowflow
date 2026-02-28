"""
Integration tests for microservice HTTP endpoints.
Uses Flask test client — no Docker or network needed.
"""

import json
import os
import sys
import tempfile

import pytest
from prometheus_client import REGISTRY

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "services", "clean-nan-service"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "services", "delete-columns-service"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "services", "data-quality-service"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "services", "outlier-detection-service"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "services"))

from common.arrow_utils import ipc_to_table


def _clear_app_modules():
    """Clear cached 'app' modules and their Prometheus collectors."""
    # Collect metric names to unregister
    collectors_to_remove = []
    for collector in list(REGISTRY._names_to_collectors.values()):
        collectors_to_remove.append(collector)
    for collector in set(collectors_to_remove):
        try:
            REGISTRY.unregister(collector)
        except Exception:
            pass
    # Clear cached app modules
    for mod in list(sys.modules):
        if mod == "app" or mod.startswith("app."):
            del sys.modules[mod]


# ── Fixtures: Flask test clients ──

@pytest.fixture
def clean_nan_client(tmp_path, monkeypatch):
    monkeypatch.setenv("ETL_DATA_ROOT", str(tmp_path))
    # Reload path_utils so it picks up the new ETL_DATA_ROOT
    import common.path_utils as _pu
    monkeypatch.setattr(_pu, "DATA_ROOT", str(tmp_path))
    _clear_app_modules()
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "services", "clean-nan-service"))
    from app import create_app
    app = create_app()
    app.config["TESTING"] = True
    with app.test_client() as client:
        yield client


@pytest.fixture
def delete_columns_client():
    _clear_app_modules()
    service_path = os.path.join(os.path.dirname(__file__), "..", "..", "services", "delete-columns-service")
    sys.path.insert(0, service_path)
    from app import create_app
    app = create_app()
    app.config["TESTING"] = True
    with app.test_client() as client:
        yield client


@pytest.fixture
def data_quality_client():
    _clear_app_modules()
    service_path = os.path.join(os.path.dirname(__file__), "..", "..", "services", "data-quality-service")
    sys.path.insert(0, service_path)
    from app import create_app
    app = create_app()
    app.config["TESTING"] = True
    with app.test_client() as client:
        yield client


# ── Clean NaN Service Tests ──

class TestCleanNanEndpoint:
    def test_clean_nan_success(self, clean_nan_client, sample_ipc_data_with_nulls):
        resp = clean_nan_client.post(
            "/clean-nan",
            data=sample_ipc_data_with_nulls,
            headers={
                "Content-Type": "application/vnd.apache.arrow.stream",
                "X-Params": json.dumps({"dataset_name": "test_dataset"})
            },
        )
        assert resp.status_code == 200
        assert resp.content_type == "application/vnd.apache.arrow.stream"
        result_table = ipc_to_table(resp.data)
        assert result_table.num_rows < 5  # some rows removed

    def test_clean_nan_missing_dataset_name(self, clean_nan_client, sample_ipc_data):
        resp = clean_nan_client.post(
            "/clean-nan",
            data=sample_ipc_data,
            headers={
                "Content-Type": "application/vnd.apache.arrow.stream",
                "X-Params": json.dumps({})
            },
        )
        assert resp.status_code == 400

    def test_clean_nan_no_data(self, clean_nan_client):
        resp = clean_nan_client.post(
            "/clean-nan",
            data=b"",
            headers={
                "Content-Type": "application/vnd.apache.arrow.stream",
                "X-Params": json.dumps({"dataset_name": "test"})
            },
        )
        assert resp.status_code == 400

    def test_health_endpoint(self, clean_nan_client):
        resp = clean_nan_client.get("/health")
        assert resp.status_code == 200
        assert resp.get_json()["status"] == "ok"

    def test_metrics_endpoint(self, clean_nan_client):
        resp = clean_nan_client.get("/metrics")
        assert resp.status_code == 200
        assert b"clean_nan_requests_total" in resp.data
