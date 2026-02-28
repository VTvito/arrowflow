"""Unit tests for clean-nan-service business logic."""

import os
import sys

import pandas as pd
import pyarrow as pa
import pytest

# Clear cached 'app' package to avoid namespace collision between services
for _mod in list(sys.modules):
    if _mod == "app" or _mod.startswith("app."):
        del sys.modules[_mod]
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "services", "clean-nan-service"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "services"))
from app.clean import apply_transformations  # noqa: E402


class TestApplyTransformations:
    """Tests for default 'drop' strategy (backward compatible)."""

    def test_removes_rows_with_nulls(self, sample_arrow_table_with_nulls):
        cleaned, removed, total_null, total_cells = apply_transformations(sample_arrow_table_with_nulls)
        df = cleaned.to_pandas()
        assert df.isna().sum().sum() == 0
        assert removed > 0

    def test_no_nulls_unchanged(self, sample_arrow_table):
        cleaned, removed, total_null, total_cells = apply_transformations(sample_arrow_table)
        assert cleaned.num_rows == sample_arrow_table.num_rows
        assert removed == 0
        assert total_null == 0

    def test_all_nulls(self):
        df = pd.DataFrame({"a": [None, None], "b": [None, None]})
        table = pa.Table.from_pandas(df)
        cleaned, removed, total_null, total_cells = apply_transformations(table)
        assert cleaned.num_rows == 0

    def test_returns_correct_counts(self, sample_arrow_table_with_nulls):
        cleaned, removed, total_null, total_cells = apply_transformations(sample_arrow_table_with_nulls)
        assert total_cells == 5 * 5  # 5 rows x 5 columns
        assert total_null > 0
        assert isinstance(removed, int)

    def test_explicit_drop_strategy(self, sample_arrow_table_with_nulls):
        cleaned, removed, total_null, total_cells = apply_transformations(
            sample_arrow_table_with_nulls, strategy="drop"
        )
        df = cleaned.to_pandas()
        assert df.isna().sum().sum() == 0


class TestFillStrategies:
    """Tests for fill-based null handling strategies."""

    def test_fill_mean(self):
        df = pd.DataFrame({"a": [1.0, None, 3.0], "b": ["x", "y", "z"]})
        table = pa.Table.from_pandas(df)
        cleaned, handled, total_null, total_cells = apply_transformations(table, strategy="fill_mean")
        result_df = cleaned.to_pandas()
        # Numeric column 'a' should be filled with mean (2.0), string 'b' unchanged
        assert result_df["a"].isna().sum() == 0
        assert result_df["a"].iloc[1] == pytest.approx(2.0)
        assert handled == 1

    def test_fill_median(self):
        df = pd.DataFrame({"a": [1.0, None, 5.0, 10.0]})
        table = pa.Table.from_pandas(df)
        cleaned, handled, _, _ = apply_transformations(table, strategy="fill_median")
        result_df = cleaned.to_pandas()
        assert result_df["a"].isna().sum() == 0
        assert result_df["a"].iloc[1] == pytest.approx(5.0)  # median of [1, 5, 10]

    def test_fill_mode(self):
        df = pd.DataFrame({"a": ["cat", "cat", "dog", None]})
        table = pa.Table.from_pandas(df)
        cleaned, handled, _, _ = apply_transformations(table, strategy="fill_mode")
        result_df = cleaned.to_pandas()
        assert result_df["a"].iloc[3] == "cat"
        assert handled == 1

    def test_fill_value(self):
        df = pd.DataFrame({"a": [1, None, 3], "b": [None, 2, 3]})
        table = pa.Table.from_pandas(df)
        cleaned, handled, _, _ = apply_transformations(table, strategy="fill_value", fill_value=0)
        result_df = cleaned.to_pandas()
        assert result_df.isna().sum().sum() == 0
        assert result_df["a"].iloc[1] == 0
        assert result_df["b"].iloc[0] == 0

    def test_fill_value_requires_value(self):
        table = pa.Table.from_pandas(pd.DataFrame({"a": [1, None]}))
        with pytest.raises(ValueError, match="fill_value parameter is required"):
            apply_transformations(table, strategy="fill_value", fill_value=None)

    def test_ffill(self):
        df = pd.DataFrame({"a": [1.0, None, None, 4.0]})
        table = pa.Table.from_pandas(df)
        cleaned, handled, _, _ = apply_transformations(table, strategy="ffill")
        result_df = cleaned.to_pandas()
        assert result_df["a"].iloc[1] == 1.0
        assert result_df["a"].iloc[2] == 1.0

    def test_bfill(self):
        df = pd.DataFrame({"a": [None, None, 3.0, 4.0]})
        table = pa.Table.from_pandas(df)
        cleaned, handled, _, _ = apply_transformations(table, strategy="bfill")
        result_df = cleaned.to_pandas()
        assert result_df["a"].iloc[0] == 3.0
        assert result_df["a"].iloc[1] == 3.0

    def test_invalid_strategy(self):
        table = pa.Table.from_pandas(pd.DataFrame({"a": [1]}))
        with pytest.raises(ValueError, match="Invalid strategy"):
            apply_transformations(table, strategy="unknown")

    def test_columns_filter(self):
        df = pd.DataFrame({"a": [1, None, 3], "b": [None, 2, None]})
        table = pa.Table.from_pandas(df)
        cleaned, handled, _, _ = apply_transformations(
            table, strategy="fill_value", fill_value=0, columns=["a"]
        )
        result_df = cleaned.to_pandas()
        # Only column 'a' should be filled
        assert result_df["a"].isna().sum() == 0
        assert result_df["b"].isna().sum() == 2  # 'b' untouched
