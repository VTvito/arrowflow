"""Unit tests for load-data-service business logic."""

import io
import json
import os
import sys

import pandas as pd
import pyarrow.parquet as pq
import pytest

# Clear cached 'app' package to avoid namespace collision between services
for _mod in list(sys.modules):
    if _mod == "app" or _mod.startswith("app."):
        del sys.modules[_mod]
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "services", "load-data-service"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "services"))
from app import load as _load_module  # noqa: E402
from app.load import load_arrow_to_format, save_output_file  # noqa: E402


class TestLoadArrowToFormat:
    def test_csv_output(self, sample_arrow_table):
        result, fmt = load_arrow_to_format(sample_arrow_table, "csv")
        assert isinstance(result, bytes)
        assert fmt == "csv"
        csv_text = result.decode("utf-8")
        assert "Alice" in csv_text
        assert "id,name,age,salary,department" in csv_text

    def test_json_output(self, sample_arrow_table):
        result, fmt = load_arrow_to_format(sample_arrow_table, "json")
        assert isinstance(result, bytes)
        assert fmt == "json"
        data = json.loads(result.decode("utf-8"))
        assert isinstance(data, list)
        assert len(data) == 5
        assert data[0]["name"] == "Alice"

    def test_xlsx_output(self, sample_arrow_table):
        result, fmt = load_arrow_to_format(sample_arrow_table, "xlsx")
        assert isinstance(result, bytes)
        assert fmt == "xlsx"
        assert len(result) > 0
        # XLSX files start with PK (ZIP header)
        assert result[:2] == b"PK"

    def test_parquet_output(self, sample_arrow_table):
        result, fmt = load_arrow_to_format(sample_arrow_table, "parquet")
        assert isinstance(result, bytes)
        assert fmt == "parquet"
        assert len(result) > 0
        # Verify it's valid Parquet by reading it back
        table_back = pq.read_table(io.BytesIO(result))
        assert table_back.num_rows == 5
        assert table_back.num_columns == 5
        assert table_back.column_names == sample_arrow_table.column_names

    def test_parquet_roundtrip_data(self, sample_arrow_table):
        result, _ = load_arrow_to_format(sample_arrow_table, "parquet")
        table_back = pq.read_table(io.BytesIO(result))
        df_original = sample_arrow_table.to_pandas()
        df_back = table_back.to_pandas()
        pd.testing.assert_frame_equal(df_original, df_back)

    def test_unsupported_format(self, sample_arrow_table):
        with pytest.raises(ValueError, match="Unsupported format"):
            load_arrow_to_format(sample_arrow_table, "xml")

    def test_case_insensitive(self, sample_arrow_table):
        result, fmt = load_arrow_to_format(sample_arrow_table, "CSV")
        assert isinstance(result, bytes)
        assert fmt == "csv"
        assert b"Alice" in result

    def test_invalid_format_type_none(self, sample_arrow_table):
        with pytest.raises(ValueError, match="Unsupported format"):
            load_arrow_to_format(sample_arrow_table, None)

    def test_xls_normalised_to_xlsx(self, sample_arrow_table):
        """The 'xls' format should be normalised to 'xlsx' since xlsxwriter produces .xlsx."""
        result, fmt = load_arrow_to_format(sample_arrow_table, "xls")
        assert isinstance(result, bytes)
        assert fmt == "xlsx"
        assert result[:2] == b"PK"  # valid XLSX/ZIP header


class TestSaveOutputFile:
    def test_creates_file(self, tmp_path, monkeypatch):
        """save_output_file writes data and returns the file path."""
        monkeypatch.setattr(
            _load_module, "ensure_dataset_dirs",
            lambda name: (str(tmp_path / name), str(tmp_path / name / "metadata")),
        )

        data = b"id,name\n1,Alice\n"
        path = save_output_file(data, "test_ds", "csv")
        assert os.path.isfile(path)
        assert path.endswith(".csv")
        with open(path, "rb") as f:
            assert f.read() == data

    def test_filename_contains_timestamp(self, tmp_path, monkeypatch):
        """Output filename follows the step_load_<timestamp>.<ext> pattern."""
        monkeypatch.setattr(
            _load_module, "ensure_dataset_dirs",
            lambda name: (str(tmp_path / name), str(tmp_path / name / "metadata")),
        )

        path = save_output_file(b"{}", "ts_test", "json")
        basename = os.path.basename(path)
        assert basename.startswith("step_load_")
        assert basename.endswith(".json")
