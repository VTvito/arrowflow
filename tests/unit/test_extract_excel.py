"""Unit tests for extract-excel-service business logic."""

import os
import sys

import pandas as pd
import pyarrow as pa
import pytest

# Clear cached 'app' package to avoid namespace collision between services
for _mod in list(sys.modules):
    if _mod == "app" or _mod.startswith("app."):
        del sys.modules[_mod]
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "services", "extract-excel-service"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "services"))
from app import extract as _extract_module  # noqa: E402
from app.extract import process_excel  # noqa: E402


@pytest.fixture(autouse=True)
def _bypass_path_security(monkeypatch):
    """Allow test tmp_path locations that are outside /app/data."""
    monkeypatch.setattr(_extract_module, "resolve_input_path", lambda fp, **kw: fp)


class TestProcessExcel:
    def test_basic_xlsx_load(self, tmp_path):
        xlsx_file = tmp_path / "test.xlsx"
        df = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"], "age": [30, 25]})
        df.to_excel(str(xlsx_file), index=False)
        table = process_excel(str(xlsx_file))
        assert isinstance(table, pa.Table)
        assert table.num_rows == 2
        assert table.num_columns == 3

    def test_xlsx_preserves_column_names(self, tmp_path):
        xlsx_file = tmp_path / "cols.xlsx"
        df = pd.DataFrame({"Alpha": [1], "Beta": [2], "Gamma": [3]})
        df.to_excel(str(xlsx_file), index=False)
        table = process_excel(str(xlsx_file))
        assert table.column_names == ["Alpha", "Beta", "Gamma"]

    def test_xlsx_with_nulls(self, tmp_path):
        xlsx_file = tmp_path / "nulls.xlsx"
        df = pd.DataFrame({"id": [1, 2, 3], "value": [100, None, 300]})
        df.to_excel(str(xlsx_file), index=False)
        table = process_excel(str(xlsx_file))
        result_df = table.to_pandas()
        assert result_df["value"].isna().sum() == 1

    def test_unsupported_extension_raises_error(self, tmp_path):
        bad_file = tmp_path / "data.txt"
        bad_file.write_text("not an excel file")
        with pytest.raises(ValueError, match="not supported"):
            process_excel(str(bad_file))

    def test_nonexistent_file_raises_error(self):
        with pytest.raises((ValueError, FileNotFoundError)):
            process_excel("/app/data/nonexistent/path/file.xlsx")

    def test_empty_xlsx(self, tmp_path):
        xlsx_file = tmp_path / "empty.xlsx"
        df = pd.DataFrame({"col_a": pd.Series(dtype="float"), "col_b": pd.Series(dtype="str")})
        df.to_excel(str(xlsx_file), index=False)
        table = process_excel(str(xlsx_file))
        assert table.num_rows == 0
        assert table.num_columns == 2
