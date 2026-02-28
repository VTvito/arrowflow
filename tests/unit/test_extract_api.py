"""Unit tests for extract-api-service business logic."""

import os
import sys
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

# Clear cached 'app' package to avoid namespace collision between services
for _mod in list(sys.modules):
    if _mod == "app" or _mod.startswith("app."):
        del sys.modules[_mod]
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "services", "extract-api-service"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "services"))
from app import extract as _api_extract_module  # noqa: E402
from app.extract import _is_private_ip, _validate_api_url, extract_from_api  # noqa: E402


# ── URL validation ───────────────────────────────────────────────────────────

class TestValidateApiUrl:
    def test_rejects_non_http_scheme(self):
        with pytest.raises(ValueError, match="Only http/https"):
            _validate_api_url("ftp://example.com/data")

    def test_rejects_missing_hostname(self):
        with pytest.raises(ValueError, match="missing hostname"):
            _validate_api_url("http://")

    def test_accepts_valid_https_url(self):
        # Will attempt DNS resolution — use a known public domain
        try:
            result = _validate_api_url("https://httpbin.org/get")
            assert result == "https://httpbin.org/get"
        except ValueError:
            # DNS resolution may fail in isolated environments
            pytest.skip("DNS resolution unavailable")


class TestIsPrivateIp:
    def test_loopback_is_private(self):
        assert _is_private_ip("127.0.0.1") is True

    def test_private_range_is_private(self):
        assert _is_private_ip("192.168.1.1") is True
        assert _is_private_ip("10.0.0.1") is True

    def test_public_ip_is_not_private(self):
        assert _is_private_ip("8.8.8.8") is False


# ── API extraction ───────────────────────────────────────────────────────────

class TestExtractFromApi:
    @patch.object(_api_extract_module, "requests")
    @patch.object(_api_extract_module, "_validate_api_url", side_effect=lambda u: u)
    def test_successful_api_call(self, mock_validate, mock_requests):
        mock_response = MagicMock()
        mock_response.json.return_value = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
        ]
        mock_response.raise_for_status = MagicMock()
        mock_requests.get.return_value = mock_response

        table = extract_from_api("https://api.example.com/users", {})
        assert isinstance(table, pa.Table)
        assert table.num_rows == 2
        assert "id" in table.column_names
        assert "name" in table.column_names

    @patch.object(_api_extract_module, "requests")
    @patch.object(_api_extract_module, "_validate_api_url", side_effect=lambda u: u)
    def test_api_with_auth_header(self, mock_validate, mock_requests):
        mock_response = MagicMock()
        mock_response.json.return_value = [{"data": "value"}]
        mock_response.raise_for_status = MagicMock()
        mock_requests.get.return_value = mock_response

        extract_from_api("https://api.example.com/data", {}, auth_type="api_key", auth_value="my-key")
        call_kwargs = mock_requests.get.call_args
        assert call_kwargs.kwargs.get("headers", {}).get("x-api-key") == "my-key"

    @patch.object(_api_extract_module, "_validate_api_url", side_effect=lambda u: u)
    def test_unsupported_auth_type_raises(self, mock_validate):
        with pytest.raises(ValueError, match="not supported"):
            extract_from_api("https://api.example.com", {}, auth_type="oauth2", auth_value="token")

    @patch.object(_api_extract_module, "requests")
    @patch.object(_api_extract_module, "_validate_api_url", side_effect=lambda u: u)
    def test_api_params_forwarded(self, mock_validate, mock_requests):
        mock_response = MagicMock()
        mock_response.json.return_value = [{"x": 1}]
        mock_response.raise_for_status = MagicMock()
        mock_requests.get.return_value = mock_response

        extract_from_api("https://api.example.com/data", {"limit": 10, "offset": 0})
        call_kwargs = mock_requests.get.call_args
        assert call_kwargs.kwargs.get("params") == {"limit": 10, "offset": 0}
