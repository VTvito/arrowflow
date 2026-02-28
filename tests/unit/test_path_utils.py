"""Unit tests for common/path_utils.py."""

import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "services"))
from common.path_utils import ensure_dataset_dirs, sanitize_dataset_name


class TestSanitizeDatasetName:
    def test_valid_name(self):
        assert sanitize_dataset_name("dataset_01") == "dataset_01"

    def test_disallow_dot_dot(self):
        with pytest.raises(ValueError, match="not allowed"):
            sanitize_dataset_name("..")


class TestEnsureDatasetDirs:
    def test_creates_dirs_under_data_root(self, tmp_path, monkeypatch):
        monkeypatch.setattr("common.path_utils.DATA_ROOT", str(tmp_path))
        dataset_folder, metadata_dir = ensure_dataset_dirs("demo")

        assert dataset_folder.startswith(str(tmp_path))
        assert metadata_dir.startswith(str(tmp_path))
        assert Path(metadata_dir).exists()

    def test_rejects_escape_name(self, tmp_path, monkeypatch):
        monkeypatch.setattr("common.path_utils.DATA_ROOT", str(tmp_path))
        with pytest.raises(ValueError):
            ensure_dataset_dirs("..")
