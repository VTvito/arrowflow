"""Unit tests for delete-columns-service business logic."""

import os
import sys

# Clear cached 'app' package to avoid namespace collision between services
for _mod in list(sys.modules):
    if _mod == "app" or _mod.startswith("app."):
        del sys.modules[_mod]
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "services", "delete-columns-service"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "services"))
from app.columns import drop_columns_arrow  # noqa: E402


class TestDropColumnsArrow:
    def test_drop_single_column(self, sample_arrow_table):
        result, removed = drop_columns_arrow(sample_arrow_table, ["age"])
        assert "age" not in result.column_names
        assert removed == 1
        assert result.num_columns == sample_arrow_table.num_columns - 1

    def test_drop_multiple_columns(self, sample_arrow_table):
        result, removed = drop_columns_arrow(sample_arrow_table, ["age", "salary"])
        assert "age" not in result.column_names
        assert "salary" not in result.column_names
        assert removed == 2

    def test_drop_nonexistent_column(self, sample_arrow_table):
        result, removed = drop_columns_arrow(sample_arrow_table, ["nonexistent"])
        assert removed == 0
        assert result.num_columns == sample_arrow_table.num_columns

    def test_drop_mix_existing_and_nonexistent(self, sample_arrow_table):
        result, removed = drop_columns_arrow(sample_arrow_table, ["age", "nonexistent"])
        assert removed == 1
        assert "age" not in result.column_names

    def test_drop_all_columns(self, sample_arrow_table):
        all_cols = sample_arrow_table.column_names
        result, removed = drop_columns_arrow(sample_arrow_table, all_cols)
        assert removed == len(all_cols)
        assert result.num_columns == 0

    def test_empty_columns_list(self, sample_arrow_table):
        result, removed = drop_columns_arrow(sample_arrow_table, [])
        assert removed == 0
        assert result.num_columns == sample_arrow_table.num_columns

    def test_none_columns(self, sample_arrow_table):
        result, removed = drop_columns_arrow(sample_arrow_table, None)
        assert removed == 0
        assert result.num_columns == sample_arrow_table.num_columns

    def test_single_string_column(self, sample_arrow_table):
        result, removed = drop_columns_arrow(sample_arrow_table, "age")
        assert removed == 1
        assert "age" not in result.column_names
