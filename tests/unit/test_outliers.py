"""Unit tests for outlier-detection-service business logic."""

import os
import sys

import pandas as pd
import pyarrow as pa
import pytest

# Clear cached 'app' package to avoid namespace collision between services
for _mod in list(sys.modules):
    if _mod == "app" or _mod.startswith("app."):
        del sys.modules[_mod]
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "services", "outlier-detection-service"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "services"))
from app.outliers import detect_and_remove_outliers  # noqa: E402


class TestDetectAndRemoveOutliers:
    def test_removes_outliers(self, sample_arrow_table_with_outliers):
        cleaned, removed = detect_and_remove_outliers(
            sample_arrow_table_with_outliers, column="salary", z_threshold=3.0
        )
        assert removed > 0
        assert cleaned.num_rows < sample_arrow_table_with_outliers.num_rows

    def test_no_outliers_when_threshold_high(self, sample_arrow_table):
        cleaned, removed = detect_and_remove_outliers(
            sample_arrow_table, column="salary", z_threshold=100.0
        )
        assert removed == 0
        assert cleaned.num_rows == sample_arrow_table.num_rows

    def test_nonexistent_column(self, sample_arrow_table):
        cleaned, removed = detect_and_remove_outliers(
            sample_arrow_table, column="nonexistent", z_threshold=3.0
        )
        assert removed == 0
        assert cleaned.num_rows == sample_arrow_table.num_rows

    def test_constant_column_no_removal(self):
        """When std=0, no outliers should be removed."""
        df = pd.DataFrame({"value": [5.0] * 10})
        table = pa.Table.from_pandas(df)
        cleaned, removed = detect_and_remove_outliers(table, column="value", z_threshold=3.0)
        assert removed == 0
        assert cleaned.num_rows == 10

    def test_single_row(self):
        df = pd.DataFrame({"value": [42.0]})
        table = pa.Table.from_pandas(df)
        cleaned, removed = detect_and_remove_outliers(table, column="value", z_threshold=3.0)
        assert removed == 0

    def test_non_numeric_column_raises(self):
        df = pd.DataFrame({"value": ["a", "b", "c"]})
        table = pa.Table.from_pandas(df)
        with pytest.raises(ValueError, match="numeric"):
            detect_and_remove_outliers(table, column="value", z_threshold=3.0)

    def test_invalid_threshold_raises(self, sample_arrow_table):
        with pytest.raises(ValueError, match="z_threshold"):
            detect_and_remove_outliers(sample_arrow_table, column="salary", z_threshold=0)
