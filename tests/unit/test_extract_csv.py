"""Unit tests for extract-csv-service business logic."""

import os
import sys

import pandas as pd
import pyarrow as pa
import pytest

# Clear cached 'app' package to avoid namespace collision between services
for _mod in list(sys.modules):
    if _mod == "app" or _mod.startswith("app."):
        del sys.modules[_mod]
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "services", "extract-csv-service"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "services"))
from app import read as _read_module  # noqa: E402
from app.read import load_csv_to_arrow  # noqa: E402


@pytest.fixture(autouse=True)
def _bypass_path_security(monkeypatch):
    """Allow test tmp_path locations that are outside /app/data."""
    monkeypatch.setattr(_read_module, "resolve_input_path", lambda fp, **kw: fp)


class TestLoadCsvToArrow:
    def test_basic_csv_load(self, tmp_path):
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("id,name,age\n1,Alice,30\n2,Bob,25\n")
        table = load_csv_to_arrow(str(csv_file))
        assert isinstance(table, pa.Table)
        assert table.num_rows == 2
        assert table.num_columns == 3
        assert table.column_names == ["id", "name", "age"]

    def test_csv_with_numeric_columns(self, tmp_path):
        csv_file = tmp_path / "nums.csv"
        csv_file.write_text("a,b,c\n1.5,2.0,3\n4.5,5.0,6\n")
        table = load_csv_to_arrow(str(csv_file))
        df = table.to_pandas()
        assert df["a"].dtype.kind == "f"  # float
        assert df["c"].dtype.kind in ("i", "f")  # int or float

    def test_csv_with_nulls(self, tmp_path):
        csv_file = tmp_path / "nulls.csv"
        csv_file.write_text("id,value\n1,100\n2,\n3,300\n")
        table = load_csv_to_arrow(str(csv_file))
        assert table.num_rows == 3
        df = table.to_pandas()
        assert df["value"].isna().sum() == 1

    def test_empty_csv_returns_table_with_columns(self, tmp_path):
        csv_file = tmp_path / "empty.csv"
        csv_file.write_text("col_a,col_b\n")
        table = load_csv_to_arrow(str(csv_file))
        assert table.num_rows == 0
        assert table.num_columns == 2

    def test_nonexistent_file_raises_error(self, monkeypatch):
        # Restore real resolve_input_path for this test
        from common.path_utils import resolve_input_path
        monkeypatch.setattr(_read_module, "resolve_input_path", resolve_input_path)
        with pytest.raises(Exception):
            load_csv_to_arrow("/nonexistent/path/file.csv")

    def test_large_csv(self, tmp_path):
        csv_file = tmp_path / "large.csv"
        rows = 10_000
        df = pd.DataFrame({"id": range(rows), "value": range(rows)})
        df.to_csv(str(csv_file), index=False)
        table = load_csv_to_arrow(str(csv_file))
        assert table.num_rows == rows
