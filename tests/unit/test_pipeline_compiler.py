"""Unit tests for PipelineCompiler — no HTTP, no Docker required."""

import os
import sys
from unittest.mock import MagicMock

import pytest

PROJECT_ROOT = os.path.join(os.path.dirname(__file__), "..", "..")
sys.path.insert(0, PROJECT_ROOT)

from ai_agent.pipeline_compiler import PipelineCompiler, _topological_layers  # noqa: E402

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


# ── _dispatch_step: service dispatch ─────────────────────────────────────────

class TestDispatchStep:
    def test_extract_csv_calls_correct_method(self):
        compiler, prep = _make_compiler()
        result = compiler._dispatch_step(
            "extract_csv", {"file_path": "/data/file.csv"}, None, "my_dataset"
        )
        prep.extract_csv.assert_called_once_with(dataset_name="my_dataset", file_path="/data/file.csv")
        assert result == FAKE_IPC

    def test_extract_excel_calls_correct_method(self):
        compiler, prep = _make_compiler()
        compiler._dispatch_step(
            "extract_excel", {"file_path": "/data/file.xlsx"}, None, "my_dataset"
        )
        prep.extract_excel.assert_called_once_with(dataset_name="my_dataset", file_path="/data/file.xlsx")

    def test_extract_api_calls_correct_method(self):
        compiler, prep = _make_compiler()
        compiler._dispatch_step(
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
        compiler._dispatch_step("clean_nan", {}, FAKE_IPC, "ds")
        prep.clean_nan.assert_called_once_with(
            FAKE_IPC, dataset_name="ds", strategy="drop", fill_value=None, columns=None
        )

    def test_clean_nan_passes_strategy_and_columns(self):
        compiler, prep = _make_compiler()
        compiler._dispatch_step(
            "clean_nan", {"strategy": "fill_median", "columns": ["age", "salary"]},
            FAKE_IPC, "ds",
        )
        prep.clean_nan.assert_called_once_with(
            FAKE_IPC, dataset_name="ds",
            strategy="fill_median", fill_value=None, columns=["age", "salary"],
        )

    def test_delete_columns_calls_correct_method(self):
        compiler, prep = _make_compiler()
        compiler._dispatch_step("delete_columns", {"columns": ["col_a", "col_b"]}, FAKE_IPC, "ds")
        prep.delete_columns.assert_called_once_with(
            FAKE_IPC, columns=["col_a", "col_b"], dataset_name="ds"
        )

    def test_data_quality_default_fail_on_errors_false(self):
        compiler, prep = _make_compiler()
        compiler._dispatch_step("data_quality", {"rules": {"min_rows": 10}}, FAKE_IPC, "ds")
        prep.check_quality.assert_called_once_with(
            FAKE_IPC, dataset_name="ds",
            rules={"min_rows": 10}, fail_on_errors=False,
        )

    def test_data_quality_with_fail_on_errors_true(self):
        compiler, prep = _make_compiler()
        compiler._dispatch_step(
            "data_quality", {"rules": {}, "fail_on_errors": True}, FAKE_IPC, "ds"
        )
        prep.check_quality.assert_called_once_with(
            FAKE_IPC, dataset_name="ds", rules={}, fail_on_errors=True,
        )

    def test_outlier_detection_calls_correct_method(self):
        compiler, prep = _make_compiler()
        compiler._dispatch_step(
            "outlier_detection", {"column": "salary", "z_threshold": 2.5}, FAKE_IPC, "ds"
        )
        prep.detect_outliers.assert_called_once_with(
            FAKE_IPC, dataset_name="ds", column="salary", z_threshold=2.5,
        )

    def test_load_data_calls_correct_method(self):
        compiler, prep = _make_compiler()
        compiler._dispatch_step("load_data", {"format": "parquet"}, FAKE_IPC, "ds")
        prep.load_data.assert_called_once_with(FAKE_IPC, format="parquet", dataset_name="ds")

    def test_unknown_service_raises_value_error(self):
        compiler, prep = _make_compiler()
        with pytest.raises(ValueError, match="Unknown service"):
            compiler._dispatch_step("does_not_exist", {}, FAKE_IPC, "ds")

    def test_join_datasets_requires_two_inputs(self):
        compiler, prep = _make_compiler()
        with pytest.raises(ValueError, match="join_datasets requires exactly 2"):
            compiler._dispatch_step("join_datasets", {"join_key": "id"}, FAKE_IPC, "ds")

    def test_join_datasets_with_two_inputs(self):
        compiler, prep = _make_compiler()
        compiler._dispatch_step(
            "join_datasets", {"join_key": "id", "join_type": "left"},
            FAKE_IPC, "ds", input_data_2=FAKE_IPC_2,
        )
        prep.join_datasets.assert_called_once_with(
            FAKE_IPC, FAKE_IPC_2, dataset_name="ds", join_key="id", join_type="left",
        )


# ── Topological layers ───────────────────────────────────────────────────────

class TestTopologicalLayers:
    def test_linear_pipeline_produces_one_step_per_layer(self):
        steps = [
            {"id": "a", "service": "extract_csv"},
            {"id": "b", "service": "clean_nan", "depends_on": ["a"]},
            {"id": "c", "service": "load_data", "depends_on": ["b"]},
        ]
        layers = _topological_layers(steps)
        assert len(layers) == 3
        assert [len(l) for l in layers] == [1, 1, 1]

    def test_parallel_extracts_grouped_in_same_layer(self):
        steps = [
            {"id": "ext1", "service": "extract_csv"},
            {"id": "ext2", "service": "extract_excel"},
            {"id": "join", "service": "join_datasets", "depends_on": ["ext1", "ext2"]},
            {"id": "save", "service": "load_data", "depends_on": ["join"]},
        ]
        layers = _topological_layers(steps)
        assert len(layers) == 3
        # First layer has both extracts
        first_ids = {s["id"] for s in layers[0]}
        assert first_ids == {"ext1", "ext2"}

    def test_diamond_dependency_produces_correct_layers(self):
        steps = [
            {"id": "a", "service": "extract_csv"},
            {"id": "b", "service": "clean_nan", "depends_on": ["a"]},
            {"id": "c", "service": "delete_columns", "depends_on": ["a"]},
            {"id": "d", "service": "load_data", "depends_on": ["b", "c"]},
        ]
        layers = _topological_layers(steps)
        assert len(layers) == 3
        middle_ids = {s["id"] for s in layers[1]}
        assert middle_ids == {"b", "c"}

    def test_cycle_raises_value_error(self):
        steps = [
            {"id": "a", "service": "extract_csv", "depends_on": ["b"]},
            {"id": "b", "service": "clean_nan", "depends_on": ["a"]},
        ]
        with pytest.raises(ValueError, match="cycle"):
            _topological_layers(steps)


# ── register_service ─────────────────────────────────────────────────────────

class TestRegisterService:
    def test_register_custom_service_and_dispatch(self):
        compiler, prep = _make_compiler()
        custom_handler = MagicMock(return_value=b"custom_output")
        compiler.register_service("my_custom_service", custom_handler)
        result = compiler._dispatch_step("my_custom_service", {"key": "val"}, FAKE_IPC, "ds")
        custom_handler.assert_called_once_with({"key": "val"}, FAKE_IPC, "ds", None)
        assert result == b"custom_output"

    def test_register_overwrites_existing(self):
        compiler, prep = _make_compiler()
        custom = MagicMock(return_value=b"overridden")
        compiler.register_service("extract_csv", custom)
        result = compiler._dispatch_step("extract_csv", {"file_path": "/x"}, None, "ds")
        custom.assert_called_once()
        assert result == b"overridden"


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

    def test_parallel_extracts_both_execute(self):
        """Two independent extracts should both execute and feed a join."""
        compiler, prep = _make_compiler()
        prep.extract_csv.return_value = b"csv_data"
        prep.extract_excel.return_value = b"excel_data"
        pipeline = _pipeline([
            {"id": "ext_csv", "service": "extract_csv", "params": {"file_path": "/a.csv"}},
            {"id": "ext_xl", "service": "extract_excel", "params": {"file_path": "/b.xlsx"}},
            {"id": "merge", "service": "join_datasets", "params": {"join_key": "id"},
             "depends_on": ["ext_csv", "ext_xl"]},
            {"id": "save", "service": "load_data", "params": {"format": "csv"},
             "depends_on": ["merge"]},
        ])
        result = compiler.execute(pipeline)
        assert result.status == "completed"
        assert len(result.steps) == 4
        prep.extract_csv.assert_called_once()
        prep.extract_excel.assert_called_once()
        prep.join_datasets.assert_called_once()
