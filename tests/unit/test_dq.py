"""Unit tests for data-quality-service business logic."""

import os
import sys

import pandas as pd
import pyarrow as pa

# Clear cached 'app' package to avoid namespace collision between services
for _mod in list(sys.modules):
    if _mod == "app" or _mod.startswith("app."):
        del sys.modules[_mod]
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "services", "data-quality-service"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "services"))
from app.dq import basic_quality_checks  # noqa: E402


class TestBasicQualityChecks:
    def test_min_rows_pass(self, sample_arrow_table):
        result = basic_quality_checks(sample_arrow_table, rules={"min_rows": 3})
        assert result["checks"]["min_rows"] is True
        assert result["rows"] == 5
        assert result["cols"] == 5

    def test_min_rows_fail(self, sample_arrow_table):
        result = basic_quality_checks(sample_arrow_table, rules={"min_rows": 100})
        assert result["checks"]["min_rows"] is False

    def test_null_ratio_pass(self, sample_arrow_table):
        result = basic_quality_checks(sample_arrow_table, rules={
            "check_null_ratio": True,
            "threshold_null_ratio": 0.5
        })
        assert result["checks"]["null_ratio"]["pass"] is True
        assert result["checks"]["null_ratio"]["value"] == 0.0

    def test_null_ratio_fail(self, sample_arrow_table_with_nulls):
        result = basic_quality_checks(sample_arrow_table_with_nulls, rules={
            "check_null_ratio": True,
            "threshold_null_ratio": 0.01  # very strict
        })
        assert result["checks"]["null_ratio"]["pass"] is False
        assert result["checks"]["null_ratio"]["value"] > 0.01

    def test_no_rules(self, sample_arrow_table):
        result = basic_quality_checks(sample_arrow_table)
        assert result["rows"] == 5
        assert result["checks"]["min_rows"] is True  # default min_rows=0

    def test_empty_table(self):
        df = pd.DataFrame({"a": []})
        table = pa.Table.from_pandas(df)
        result = basic_quality_checks(table, rules={"min_rows": 1})
        assert result["checks"]["min_rows"] is False
        assert result["rows"] == 0


class TestDuplicateCheck:
    def test_no_duplicates(self, sample_arrow_table):
        result = basic_quality_checks(sample_arrow_table, rules={"check_duplicates": True})
        assert result["checks"]["duplicates"]["pass"] is True
        assert result["checks"]["duplicates"]["duplicate_rows"] == 0

    def test_with_duplicates(self):
        df = pd.DataFrame({"a": [1, 1, 2], "b": ["x", "x", "y"]})
        table = pa.Table.from_pandas(df)
        result = basic_quality_checks(table, rules={"check_duplicates": True})
        assert result["checks"]["duplicates"]["pass"] is False
        assert result["checks"]["duplicates"]["duplicate_rows"] == 1


class TestColumnTypeCheck:
    def test_numeric_type_pass(self, sample_arrow_table):
        result = basic_quality_checks(sample_arrow_table, rules={
            "check_column_types": {"age": "numeric", "salary": "numeric"}
        })
        assert result["checks"]["column_types"]["age"]["pass"] is True
        assert result["checks"]["column_types"]["salary"]["pass"] is True

    def test_string_type_pass(self, sample_arrow_table):
        result = basic_quality_checks(sample_arrow_table, rules={
            "check_column_types": {"name": "string"}
        })
        assert result["checks"]["column_types"]["name"]["pass"] is True

    def test_type_mismatch(self, sample_arrow_table):
        result = basic_quality_checks(sample_arrow_table, rules={
            "check_column_types": {"name": "numeric"}
        })
        assert result["checks"]["column_types"]["name"]["pass"] is False

    def test_missing_column(self, sample_arrow_table):
        result = basic_quality_checks(sample_arrow_table, rules={
            "check_column_types": {"nonexistent": "string"}
        })
        assert result["checks"]["column_types"]["nonexistent"]["pass"] is False
        assert result["checks"]["column_types"]["nonexistent"]["actual"] == "missing"


class TestUniqueColumnCheck:
    def test_unique_column_pass(self, sample_arrow_table):
        result = basic_quality_checks(sample_arrow_table, rules={
            "check_unique_columns": ["id"]
        })
        assert result["checks"]["unique_columns"]["id"]["pass"] is True

    def test_non_unique_column(self):
        df = pd.DataFrame({"a": [1, 1, 2]})
        table = pa.Table.from_pandas(df)
        result = basic_quality_checks(table, rules={"check_unique_columns": ["a"]})
        assert result["checks"]["unique_columns"]["a"]["pass"] is False

    def test_missing_column(self, sample_arrow_table):
        result = basic_quality_checks(sample_arrow_table, rules={
            "check_unique_columns": ["nonexistent"]
        })
        assert result["checks"]["unique_columns"]["nonexistent"]["pass"] is False


class TestValueRangeCheck:
    def test_range_pass(self, sample_arrow_table):
        result = basic_quality_checks(sample_arrow_table, rules={
            "check_value_range": {"age": {"min": 0, "max": 200}}
        })
        assert result["checks"]["value_range"]["age"]["pass"] is True

    def test_range_fail(self, sample_arrow_table):
        result = basic_quality_checks(sample_arrow_table, rules={
            "check_value_range": {"age": {"min": 30, "max": 35}}
        })
        assert result["checks"]["value_range"]["age"]["pass"] is False

    def test_non_numeric_column(self, sample_arrow_table):
        result = basic_quality_checks(sample_arrow_table, rules={
            "check_value_range": {"name": {"min": 0, "max": 100}}
        })
        assert result["checks"]["value_range"]["name"]["pass"] is False


class TestCompletenessCheck:
    def test_full_completeness(self, sample_arrow_table):
        result = basic_quality_checks(sample_arrow_table, rules={"check_completeness": True})
        assert "completeness" in result["checks"]
        for col, info in result["checks"]["completeness"].items():
            assert info["ratio"] == 1.0

    def test_partial_completeness(self, sample_arrow_table_with_nulls):
        result = basic_quality_checks(sample_arrow_table_with_nulls, rules={"check_completeness": True})
        completeness = result["checks"]["completeness"]
        # At least one column should have ratio < 1.0
        has_incomplete = any(info["ratio"] < 1.0 for info in completeness.values())
        assert has_incomplete
