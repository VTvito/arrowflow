"""Unit tests for PipelineCompiler — no HTTP, no Docker required."""

import os
import sys
from unittest.mock import MagicMock

import pytest

PROJECT_ROOT = os.path.join(os.path.dirname(__file__), "..", "..")
sys.path.insert(0, PROJECT_ROOT)

from ai_agent.pipeline_compiler import PipelineCompiler  # noqa: E402

# ── Helpers ───────────────────────────────────────────────────────────────────

FAKE_IPC = b"fake_arrow_ipc_bytes"
FAKE_IPC_2 = b"fake_arrow_ipc_bytes_2"


def _make_compiler():
    """Return a PipelineCompiler wired to a MagicMock Preparator."""
    prep = MagicMock()
    prep.correlation_id = "test-correlation-id"
    # Every Preparator method returns fake IPC bytes by default
    prep.extract_csv.return_value = FAKE_IPC
    prep.extract_excel.return_value = FAKE_IPC
    prep.extract_api.return_value = FAKE_IPC
    prep.extract_sql.return_value = FAKE_IPC
    prep.clean_nan.return_value = FAKE_IPC
    prep.delete_columns.return_value = FAKE_IPC
    prep.check_quality.return_value = FAKE_IPC
    prep.detect_outliers.return_value = FAKE_IPC
    prep.join_datasets.return_value = FAKE_IPC
    prep.text_completion_llm.return_value = FAKE_IPC
    prep.load_data.return_value = b'{"status": "ok"}'
    return PipelineCompiler(prep), prep


def _pipeline(steps, name="test_pipeline"):
    return {"pipeline": {"name": name, "description": "test", "steps": steps}}


# ── _execute_step: service dispatch ──────────────────────────────────────────

class TestExecuteStepDispatch:
    def test_extract_csv_calls_correct_method(self):
        compiler, prep = _make_compiler()
        result = compiler._execute_step(
            "extract_csv", {"file_path": "/data/file.csv"}, None, "my_dataset"
        )
        prep.extract_csv.assert_called_once_with(dataset_name="my_dataset", file_path="/data/file.csv")
        assert result == FAKE_IPC

    def test_extract_excel_calls_correct_method(self):
        compiler, prep = _make_compiler()
        compiler._execute_step(
            "extract_excel", {"file_path": "/data/file.xlsx"}, None, "my_dataset"
        )
        prep.extract_excel.assert_called_once_with(dataset_name="my_dataset", file_path="/data/file.xlsx")

    def test_extract_api_calls_correct_method(self):
        compiler, prep = _make_compiler()
        compiler._execute_step(
            "extract_api",
            {"api_url": "https://api.example.com/data", "api_params": {"limit": 100}},
            None, "my_dataset",
        )
        prep.extract_api.assert_called_once_with(
            dataset_name="my_dataset",
            api_url="https://api.example.com/data",
            api_params={"limit": 100},
            auth_type=None,
            auth_value=None,
        )

    def test_clean_nan_calls_correct_method_with_defaults(self):
        compiler, prep = _make_compiler()
        compiler._execute_step("clean_nan", {}, FAKE_IPC, "ds")
        prep.clean_nan.assert_called_once_with(
            FAKE_IPC, dataset_name="ds", strategy="drop", fill_value=None, columns=None
        )

    def test_clean_nan_passes_strategy_and_columns(self):
        compiler, prep = _make_compiler()
        compiler._execute_step(
            "clean_nan", {"strategy": "fill_median", "columns": ["age", "salary"]},
            FAKE_IPC, "ds",
        )
        prep.clean_nan.assert_called_once_with(
            FAKE_IPC, dataset_name="ds",
            strategy="fill_median", fill_value=None, columns=["age", "salary"],
        )

    def test_delete_columns_calls_correct_method(self):
        compiler, prep = _make_compiler()
        compiler._execute_step("delete_columns", {"columns": ["col_a", "col_b"]}, FAKE_IPC, "ds")
        prep.delete_columns.assert_called_once_with(
            FAKE_IPC, columns=["col_a", "col_b"], dataset_name="ds"
        )

    def test_data_quality_default_fail_on_errors_false(self):
        compiler, prep = _make_compiler()
        compiler._execute_step("data_quality", {"rules": {"min_rows": 10}}, FAKE_IPC, "ds")
        prep.check_quality.assert_called_once_with(
            FAKE_IPC, dataset_name="ds",
            rules={"min_rows": 10}, fail_on_errors=False,
        )

    def test_data_quality_with_fail_on_errors_true(self):
        compiler, prep = _make_compiler()
        compiler._execute_step(
            "data_quality", {"rules": {}, "fail_on_errors": True}, FAKE_IPC, "ds"
        )
        prep.check_quality.assert_called_once_with(
            FAKE_IPC, dataset_name="ds", rules={}, fail_on_errors=True,
        )

    def test_outlier_detection_calls_correct_method(self):
        compiler, prep = _make_compiler()
        compiler._execute_step(
            "outlier_detection", {"column": "salary", "z_threshold": 2.5}, FAKE_IPC, "ds"
        )
        prep.detect_outliers.assert_called_once_with(
            FAKE_IPC, dataset_name="ds", column="salary", z_threshold=2.5,
        )

    def test_load_data_calls_correct_method(self):
        compiler, prep = _make_compiler()
        compiler._execute_step("load_data", {"format": "parquet"}, FAKE_IPC, "ds")
        prep.load_data.assert_called_once_with(FAKE_IPC, format="parquet", dataset_name="ds")

    def test_unknown_service_raises_value_error(self):
        compiler, prep = _make_compiler()
        with pytest.raises(ValueError, match="Unknown service"):
            compiler._execute_step("does_not_exist", {}, FAKE_IPC, "ds")

    def test_join_datasets_requires_two_inputs(self):
        compiler, prep = _make_compiler()
        with pytest.raises(ValueError, match="join_datasets requires exactly 2"):
            compiler._execute_step("join_datasets", {"join_key": "id"}, FAKE_IPC, "ds")

    def test_join_datasets_with_two_inputs(self):
        compiler, prep = _make_compiler()
        compiler._execute_step(
            "join_datasets", {"join_key": "id", "join_type": "left"},
            FAKE_IPC, "ds", input_data_2=FAKE_IPC_2,
        )
        prep.join_datasets.assert_called_once_with(
            FAKE_IPC, FAKE_IPC_2, dataset_name="ds", join_key="id", join_type="left",
        )


# ── execute(): full pipeline run ─────────────────────────────────────────────

class TestExecutePipeline:
    def test_successful_pipeline_returns_completed_status(self):
        compiler, prep = _make_compiler()
        pipeline = _pipeline([
            {"id": "extract", "service": "extract_csv", "params": {"file_path": "/data/f.csv"}},
            {"id": "clean", "service": "clean_nan", "depends_on": ["extract"]},
            {"id": "save", "service": "load_data", "params": {"format": "csv"}, "depends_on": ["clean"]},
        ])
        result = compiler.execute(pipeline)
        assert result.status == "completed"
        assert len(result.steps) == 3
        assert all(s.status == "success" for s in result.steps)

    def test_failed_step_stops_pipeline(self):
        compiler, prep = _make_compiler()
        prep.clean_nan.side_effect = RuntimeError("Service unavailable")
        pipeline = _pipeline([
            {"id": "extract", "service": "extract_csv", "params": {"file_path": "/data/f.csv"}},
            {"id": "clean", "service": "clean_nan", "depends_on": ["extract"]},
            {"id": "save", "service": "load_data", "params": {"format": "csv"}, "depends_on": ["clean"]},
        ])
        result = compiler.execute(pipeline)
        assert result.status == "failed"
        # Only the first two steps were attempted; load_data was skipped
        assert len(result.steps) == 2
        assert result.steps[0].status == "success"
        assert result.steps[1].status == "error"
        assert "Service unavailable" in result.steps[1].error_message

    def test_result_carries_correlation_id(self):
        compiler, prep = _make_compiler()
        pipeline = _pipeline([
            {"id": "extract", "service": "extract_csv", "params": {"file_path": "/data/f.csv"}},
            {"id": "save", "service": "load_data", "params": {"format": "csv"}, "depends_on": ["extract"]},
        ])
        result = compiler.execute(pipeline)
        assert result.correlation_id == "test-correlation-id"

    def test_step_outputs_available_after_execute(self):
        compiler, prep = _make_compiler()
        pipeline = _pipeline([
            {"id": "extract", "service": "extract_csv", "params": {"file_path": "/data/f.csv"}},
            {"id": "save", "service": "load_data", "params": {"format": "csv"}, "depends_on": ["extract"]},
        ])
        compiler.execute(pipeline)
        assert "extract" in compiler.last_step_outputs
        assert compiler.last_step_outputs["extract"] == FAKE_IPC

    def test_dependency_output_correctly_chained(self):
        """Ensure each step receives the output of its depends_on predecessor."""
        compiler, prep = _make_compiler()
        prep.extract_csv.return_value = b"extracted"
        prep.clean_nan.return_value = b"cleaned"
        pipeline = _pipeline([
            {"id": "extract", "service": "extract_csv", "params": {"file_path": "/data/f.csv"}},
            {"id": "clean", "service": "clean_nan", "depends_on": ["extract"]},
            {"id": "save", "service": "load_data", "params": {"format": "csv"}, "depends_on": ["clean"]},
        ])
        compiler.execute(pipeline)
        # clean_nan must receive the output of extract_csv
        prep.clean_nan.assert_called_once_with(
            b"extracted", dataset_name="test_pipeline",
            strategy="drop", fill_value=None, columns=None,
        )
        # load_data must receive the output of clean_nan
        prep.load_data.assert_called_once_with(
            b"cleaned", format="csv", dataset_name="test_pipeline",
        )

    def test_progress_callback_called_for_each_step(self):
        compiler, prep = _make_compiler()
        pipeline = _pipeline([
            {"id": "extract", "service": "extract_csv", "params": {"file_path": "/data/f.csv"}},
            {"id": "save", "service": "load_data", "params": {"format": "csv"}, "depends_on": ["extract"]},
        ])
        calls = []
        compiler.execute(pipeline, progress_callback=lambda step_id, status, pct: calls.append((step_id, status)))
        step_ids = [c[0] for c in calls]
        assert "extract" in step_ids
        assert "save" in step_ids

    def test_to_dict_serializable(self):
        compiler, prep = _make_compiler()
        pipeline = _pipeline([
            {"id": "extract", "service": "extract_csv", "params": {"file_path": "/data/f.csv"}},
            {"id": "save", "service": "load_data", "params": {"format": "csv"}, "depends_on": ["extract"]},
        ])
        result = compiler.execute(pipeline)
        d = result.to_dict()
        assert d["status"] == "completed"
        assert isinstance(d["total_duration_sec"], float)
        assert len(d["steps"]) == 2
