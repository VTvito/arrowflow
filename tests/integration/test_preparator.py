"""
Integration tests for the Preparator v4 SDK.
Tests the client-side logic without requiring running services.
"""

import os
import sys
from unittest.mock import MagicMock

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "preparator"))
from preparator_v4 import Preparator


class TestPreparatorInit:
    def test_creates_session_with_retry(self, services_config):
        prep = Preparator(services_config)
        assert prep.session is not None
        assert prep.timeout == (5, 300)
        prep.close()

    def test_custom_timeout(self, services_config):
        prep = Preparator(services_config, timeout=(10, 600))
        assert prep.timeout == (10, 600)
        prep.close()

    def test_context_manager(self, services_config):
        with Preparator(services_config) as prep:
            assert prep.session is not None
        # Session should be closed after exiting context

    def test_close_session(self, services_config):
        prep = Preparator(services_config)
        prep.close()
        # No assertion needed — just ensure no exception


class TestPreparatorErrorHandling:
    def test_error_response_with_json_body(self, services_config):
        prep = Preparator(services_config)
        mock_resp = MagicMock()
        mock_resp.status_code = 400
        mock_resp.raise_for_status.side_effect = __import__('requests').HTTPError(
            "400 Client Error", response=mock_resp
        )
        mock_resp.json.return_value = {"status": "error", "message": "Column 'X' not found"}
        mock_resp.text = '{"status": "error", "message": "Column \'X\' not found"}'

        with pytest.raises(__import__('requests').HTTPError, match="Column 'X' not found"):
            prep._handle_error_response(mock_resp, "test_service")
        prep.close()

    def test_error_response_without_json(self, services_config):
        prep = Preparator(services_config)
        mock_resp = MagicMock()
        mock_resp.status_code = 500
        mock_resp.raise_for_status.side_effect = __import__('requests').HTTPError(
            "500 Server Error", response=mock_resp
        )
        mock_resp.json.side_effect = ValueError("No JSON")
        mock_resp.text = "Internal Server Error"

        with pytest.raises(__import__('requests').HTTPError, match="Internal Server Error"):
            prep._handle_error_response(mock_resp, "test_service")
        prep.close()


class TestPreparatorMethods:
    def test_extract_api_default_params(self, services_config):
        """Verify mutable default arg fix — api_params defaults to None, not {}."""
        prep = Preparator(services_config)
        # Inspect the method signature
        import inspect
        sig = inspect.signature(prep.extract_api)
        default = sig.parameters["api_params"].default
        assert default is None, "api_params should default to None, not {}"
        prep.close()
