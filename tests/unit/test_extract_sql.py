"""Unit tests for extract-sql-service business logic (validation only — no DB needed)."""

import os
import sys

import pytest

# Clear cached 'app' package to avoid namespace collision between services
for _mod in list(sys.modules):
    if _mod == "app" or _mod.startswith("app."):
        del sys.modules[_mod]
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "services", "extract-sql-service"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "services"))
from app.extract import redact_db_url, validate_sql_query  # noqa: E402


# ── SQL validation ───────────────────────────────────────────────────────────

class TestValidateSqlQuery:
    def test_valid_select_query(self):
        result = validate_sql_query("SELECT * FROM users")
        assert result == "SELECT * FROM users"

    def test_valid_with_cte(self):
        result = validate_sql_query("WITH cte AS (SELECT 1) SELECT * FROM cte")
        assert "WITH" in result

    def test_strips_trailing_semicolon(self):
        result = validate_sql_query("SELECT 1;")
        assert result == "SELECT 1"

    def test_rejects_empty_query(self):
        with pytest.raises(ValueError, match="non-empty string"):
            validate_sql_query("")

    def test_rejects_none_query(self):
        with pytest.raises(ValueError, match="non-empty string"):
            validate_sql_query(None)

    def test_rejects_insert(self):
        with pytest.raises(ValueError, match="read-only|dangerous"):
            validate_sql_query("INSERT INTO users VALUES (1, 'x')")

    def test_rejects_drop(self):
        with pytest.raises(ValueError, match="read-only|dangerous"):
            validate_sql_query("DROP TABLE users")

    def test_rejects_delete(self):
        with pytest.raises(ValueError, match="read-only|dangerous"):
            validate_sql_query("DELETE FROM users WHERE id = 1")

    def test_rejects_update(self):
        with pytest.raises(ValueError, match="read-only|dangerous"):
            validate_sql_query("UPDATE users SET name = 'x'")

    def test_rejects_multiple_statements(self):
        with pytest.raises(ValueError, match="Multiple SQL statements"):
            validate_sql_query("SELECT 1; SELECT 2")

    def test_rejects_non_select_query(self):
        with pytest.raises(ValueError, match="read-only SELECT"):
            validate_sql_query("CALL my_procedure()")

    def test_select_with_subquery(self):
        query = "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)"
        result = validate_sql_query(query)
        assert "SELECT" in result

    def test_case_insensitive(self):
        result = validate_sql_query("select id from users")
        assert result == "select id from users"


# ── URL redaction ────────────────────────────────────────────────────────────

class TestRedactDbUrl:
    def test_redacts_password(self):
        url = "postgresql://user:secret@localhost:5432/mydb"
        result = redact_db_url(url)
        assert "secret" not in result
        assert "user" in result

    def test_handles_no_password(self):
        url = "sqlite:///mydb.sqlite"
        result = redact_db_url(url)
        assert "mydb.sqlite" in result

    def test_handles_invalid_url(self):
        result = redact_db_url("not-a-valid-url")
        # Should not raise, returns fallback
        assert isinstance(result, str)
