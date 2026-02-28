"""Unit tests for join-datasets-service business logic."""

import os
import sys

import pandas as pd
import pyarrow as pa
import pytest

# Clear cached 'app' package to avoid namespace collision between services
for _mod in list(sys.modules):
    if _mod == "app" or _mod.startswith("app."):
        del sys.modules[_mod]
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "services", "join-datasets-service"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "services"))
from app.join import join_datasets_logic  # noqa: E402


class TestJoinDatasetsLogic:
    @pytest.fixture
    def table_left(self):
        df = pd.DataFrame({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]})
        return pa.Table.from_pandas(df)

    @pytest.fixture
    def table_right(self):
        df = pd.DataFrame({"id": [2, 3, 4], "score": [90, 85, 70]})
        return pa.Table.from_pandas(df)

    def test_inner_join(self, table_left, table_right):
        result, shape = join_datasets_logic(table_left, table_right, join_key="id", join_type="inner")
        assert shape[0] == 2  # ids 2 and 3
        assert "name" in result.column_names
        assert "score" in result.column_names

    def test_left_join(self, table_left, table_right):
        result, shape = join_datasets_logic(table_left, table_right, join_key="id", join_type="left")
        assert shape[0] == 3  # all left rows

    def test_right_join(self, table_left, table_right):
        result, shape = join_datasets_logic(table_left, table_right, join_key="id", join_type="right")
        assert shape[0] == 3  # all right rows

    def test_outer_join(self, table_left, table_right):
        result, shape = join_datasets_logic(table_left, table_right, join_key="id", join_type="outer")
        assert shape[0] == 4  # ids 1, 2, 3, 4

    def test_no_matching_keys(self):
        df1 = pd.DataFrame({"id": [1, 2], "val": ["a", "b"]})
        df2 = pd.DataFrame({"id": [3, 4], "val2": ["c", "d"]})
        result, shape = join_datasets_logic(
            pa.Table.from_pandas(df1), pa.Table.from_pandas(df2),
            join_key="id", join_type="inner"
        )
        assert shape[0] == 0

    def test_invalid_join_type(self, table_left, table_right):
        with pytest.raises(ValueError, match="join_type"):
            join_datasets_logic(table_left, table_right, join_key="id", join_type="cross")

    def test_missing_join_key_in_left(self, table_right):
        left = pa.Table.from_pandas(pd.DataFrame({"user_id": [1, 2]}))
        with pytest.raises(ValueError, match="first dataset"):
            join_datasets_logic(left, table_right, join_key="id", join_type="inner")

    def test_empty_join_key(self, table_left, table_right):
        with pytest.raises(ValueError, match="join_key"):
            join_datasets_logic(table_left, table_right, join_key="", join_type="inner")
