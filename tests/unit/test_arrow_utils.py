"""Unit tests for common/arrow_utils.py — Arrow IPC serialization/deserialization."""

import os
import sys

import pyarrow as pa
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "services"))
from common.arrow_utils import ipc_to_table, table_to_ipc


class TestTableToIpc:
    def test_roundtrip_preserves_data(self, sample_arrow_table):
        """Serialize and deserialize should produce identical data."""
        ipc_bytes = table_to_ipc(sample_arrow_table)
        restored = ipc_to_table(ipc_bytes)

        assert restored.num_rows == sample_arrow_table.num_rows
        assert restored.num_columns == sample_arrow_table.num_columns
        assert restored.column_names == sample_arrow_table.column_names
        assert restored.to_pandas().equals(sample_arrow_table.to_pandas())

    def test_output_is_bytes(self, sample_arrow_table):
        ipc_bytes = table_to_ipc(sample_arrow_table)
        assert isinstance(ipc_bytes, bytes)
        assert len(ipc_bytes) > 0

    def test_empty_table(self):
        empty = pa.table({"col1": pa.array([], type=pa.int64()), "col2": pa.array([], type=pa.string())})
        ipc_bytes = table_to_ipc(empty)
        restored = ipc_to_table(ipc_bytes)
        assert restored.num_rows == 0
        assert restored.column_names == ["col1", "col2"]


class TestIpcToTable:
    def test_invalid_data_raises(self):
        with pytest.raises(Exception):
            ipc_to_table(b"not valid arrow data")

    def test_empty_bytes_raises(self):
        with pytest.raises(Exception):
            ipc_to_table(b"")
