"""Unit tests for the standalone validate_pipeline function and PipelineAgent."""

import os
import sys

# Add project root so ai_agent and schemas are importable
PROJECT_ROOT = os.path.join(os.path.dirname(__file__), "..", "..")
sys.path.insert(0, PROJECT_ROOT)

from ai_agent.pipeline_agent import validate_pipeline  # noqa: E402

# ── Minimal registry fixture ─────────────────────────────────────────────────

MINIMAL_REGISTRY = {
    "services": {
        "extract_csv": {
            "type": "extract",
            "params": {
                "file_path": {"type": "string", "required": True, "description": "Path to CSV"},
                "dataset_name": {"type": "string", "required": True, "description": "Dataset name"},
            },
        },
        "clean_nan": {
            "type": "transform",
            "params": {
                "dataset_name": {"type": "string", "required": True, "description": "Dataset name"},
                "strategy": {"type": "string", "required": False, "default": "drop"},
            },
        },
        "data_quality": {
            "type": "transform",
            "params": {
                "dataset_name": {"type": "string", "required": True, "description": "Dataset name"},
                "rules": {"type": "object", "required": False},
                "fail_on_errors": {"type": "boolean", "required": False, "default": False},
            },
        },
        "load_data": {
            "type": "load",
            "params": {
                "format": {"type": "string", "required": True, "description": "Output format"},
                "dataset_name": {"type": "string", "required": True, "description": "Dataset name"},
            },
        },
        "join_datasets": {
            "type": "transform",
            "params": {
                "dataset_name": {"type": "string", "required": True, "description": "Dataset name"},
                "join_key": {"type": "string", "required": False},
                "join_type": {"type": "string", "required": False},
            },
        },
    }
}


def _pipeline(steps, name="test_pipeline"):
    return {"pipeline": {"name": name, "steps": steps}}


# ── Validity tests ────────────────────────────────────────────────────────────

class TestValidPipelines:
    def test_complete_pipeline_no_errors(self):
        pipeline = _pipeline([
            {"id": "extract", "service": "extract_csv", "params": {"file_path": "/app/data/file.csv"}},
            {"id": "clean", "service": "clean_nan", "depends_on": ["extract"]},
            {"id": "save", "service": "load_data", "params": {"format": "csv"}, "depends_on": ["clean"]},
        ])
        errors, warnings = validate_pipeline(pipeline, MINIMAL_REGISTRY)
        assert errors == []
        assert warnings == []

    def test_pipeline_without_load_data_gives_warning_not_error(self):
        pipeline = _pipeline([
            {"id": "extract", "service": "extract_csv", "params": {"file_path": "/data/file.csv"}},
            {"id": "clean", "service": "clean_nan", "depends_on": ["extract"]},
        ])
        errors, warnings = validate_pipeline(pipeline, MINIMAL_REGISTRY)
        assert errors == [], "Missing load_data should NOT be an error"
        assert len(warnings) == 1
        assert "load_data" in warnings[0]

    def test_pipeline_with_quality_check_only_no_required_params(self):
        pipeline = _pipeline([
            {"id": "extract", "service": "extract_csv", "params": {"file_path": "/data/x.csv"}},
            {"id": "dq", "service": "data_quality", "depends_on": ["extract"]},
            {"id": "save", "service": "load_data", "params": {"format": "csv"}, "depends_on": ["dq"]},
        ])
        errors, warnings = validate_pipeline(pipeline, MINIMAL_REGISTRY)
        assert errors == []


# ── Error tests ───────────────────────────────────────────────────────────────

class TestInvalidPipelines:
    def test_missing_top_level_pipeline_key(self):
        errors, warnings = validate_pipeline({"steps": []}, MINIMAL_REGISTRY)
        assert any("pipeline" in e.lower() for e in errors)

    def test_none_pipeline_def(self):
        errors, warnings = validate_pipeline(None, MINIMAL_REGISTRY)
        assert any("pipeline" in e.lower() for e in errors)

    def test_empty_steps_list(self):
        errors, warnings = validate_pipeline({"pipeline": {"name": "x", "steps": []}}, MINIMAL_REGISTRY)
        assert any("steps" in e.lower() for e in errors)

    def test_missing_steps_key(self):
        errors, warnings = validate_pipeline({"pipeline": {"name": "x"}}, MINIMAL_REGISTRY)
        assert any("steps" in e.lower() for e in errors)

    def test_missing_name_field(self):
        pipeline = {"pipeline": {"steps": [
            {"id": "e", "service": "extract_csv", "params": {"file_path": "/data/f.csv"}},
            {"id": "s", "service": "load_data", "params": {"format": "csv"}, "depends_on": ["e"]},
        ]}}
        errors, warnings = validate_pipeline(pipeline, MINIMAL_REGISTRY)
        assert any("name" in e.lower() for e in errors)

    def test_no_extract_step(self):
        pipeline = _pipeline([
            {"id": "clean", "service": "clean_nan"},
            {"id": "save", "service": "load_data", "params": {"format": "csv"}, "depends_on": ["clean"]},
        ])
        errors, warnings = validate_pipeline(pipeline, MINIMAL_REGISTRY)
        assert any("extract" in e.lower() for e in errors)

    def test_unknown_service(self):
        pipeline = _pipeline([
            {"id": "extract", "service": "extract_csv", "params": {"file_path": "/data/f.csv"}},
            {"id": "nonexistent", "service": "this_does_not_exist", "depends_on": ["extract"]},
            {"id": "save", "service": "load_data", "params": {"format": "csv"}, "depends_on": ["nonexistent"]},
        ])
        errors, warnings = validate_pipeline(pipeline, MINIMAL_REGISTRY)
        assert any("this_does_not_exist" in e for e in errors)

    def test_missing_required_param(self):
        # extract_csv requires file_path
        pipeline = _pipeline([
            {"id": "extract", "service": "extract_csv", "params": {}},  # file_path missing
            {"id": "save", "service": "load_data", "params": {"format": "csv"}, "depends_on": ["extract"]},
        ])
        errors, warnings = validate_pipeline(pipeline, MINIMAL_REGISTRY)
        assert any("file_path" in e for e in errors)

    def test_missing_required_param_in_load(self):
        # load_data requires format
        pipeline = _pipeline([
            {"id": "extract", "service": "extract_csv", "params": {"file_path": "/data/f.csv"}},
            {"id": "save", "service": "load_data", "params": {}, "depends_on": ["extract"]},
        ])
        errors, warnings = validate_pipeline(pipeline, MINIMAL_REGISTRY)
        assert any("format" in e for e in errors)

    def test_depends_on_unknown_step(self):
        pipeline = _pipeline([
            {"id": "extract", "service": "extract_csv", "params": {"file_path": "/data/f.csv"}},
            {"id": "save", "service": "load_data", "params": {"format": "csv"}, "depends_on": ["ghost_step"]},
        ])
        errors, warnings = validate_pipeline(pipeline, MINIMAL_REGISTRY)
        assert any("ghost_step" in e for e in errors)

    def test_duplicate_step_id(self):
        pipeline = _pipeline([
            {"id": "extract", "service": "extract_csv", "params": {"file_path": "/data/f.csv"}},
            {"id": "extract", "service": "clean_nan", "depends_on": ["extract"]},  # duplicate id
            {"id": "save", "service": "load_data", "params": {"format": "csv"}, "depends_on": ["extract"]},
        ])
        errors, warnings = validate_pipeline(pipeline, MINIMAL_REGISTRY)
        assert any("duplicate" in e.lower() for e in errors)

    def test_step_missing_service_field(self):
        pipeline = _pipeline([
            {"id": "extract", "service": "extract_csv", "params": {"file_path": "/data/f.csv"}},
            {"id": "broken"},  # no 'service' key
            {"id": "save", "service": "load_data", "params": {"format": "csv"}, "depends_on": ["broken"]},
        ])
        errors, warnings = validate_pipeline(pipeline, MINIMAL_REGISTRY)
        assert any("service" in e.lower() for e in errors)

    def test_non_extract_requires_depends_on(self):
        pipeline = _pipeline([
            {"id": "extract", "service": "extract_csv", "params": {"file_path": "/data/f.csv"}},
            {"id": "clean", "service": "clean_nan"},
        ])
        errors, warnings = validate_pipeline(pipeline, MINIMAL_REGISTRY)
        assert any("requires depends_on" in e.lower() for e in errors)

    def test_extract_cannot_have_depends_on(self):
        pipeline = _pipeline([
            {"id": "extract", "service": "extract_csv", "params": {"file_path": "/data/f.csv"}, "depends_on": ["x"]},
        ])
        errors, warnings = validate_pipeline(pipeline, MINIMAL_REGISTRY)
        assert any("must not have depends_on" in e.lower() for e in errors)

    def test_params_must_be_dict(self):
        pipeline = _pipeline([
            {"id": "extract", "service": "extract_csv", "params": []},
        ])
        errors, warnings = validate_pipeline(pipeline, MINIMAL_REGISTRY)
        assert any("params" in e.lower() and "dict" in e.lower() for e in errors)

    def test_depends_on_must_be_list(self):
        pipeline = _pipeline([
            {"id": "extract", "service": "extract_csv", "params": {"file_path": "/data/f.csv"}},
            {"id": "clean", "service": "clean_nan", "depends_on": "extract"},
        ])
        errors, warnings = validate_pipeline(pipeline, MINIMAL_REGISTRY)
        assert any("depends_on" in e.lower() and "list" in e.lower() for e in errors)

    def test_non_join_multiple_depends_on_invalid(self):
        pipeline = _pipeline([
            {"id": "extract1", "service": "extract_csv", "params": {"file_path": "/data/a.csv"}},
            {"id": "extract2", "service": "extract_csv", "params": {"file_path": "/data/b.csv"}},
            {"id": "clean", "service": "clean_nan", "depends_on": ["extract1", "extract2"]},
        ])
        errors, warnings = validate_pipeline(pipeline, MINIMAL_REGISTRY)
        assert any("multiple depends_on" in e.lower() for e in errors)

    def test_join_requires_two_depends_on(self):
        pipeline = _pipeline([
            {"id": "extract1", "service": "extract_csv", "params": {"file_path": "/data/a.csv"}},
            {"id": "join", "service": "join_datasets", "depends_on": ["extract1"]},
        ])
        errors, warnings = validate_pipeline(pipeline, MINIMAL_REGISTRY)
        assert any("join_datasets" in e.lower() and "exactly 2" in e.lower() for e in errors)

    def test_join_accepts_two_depends_on(self):
        pipeline = _pipeline([
            {"id": "extract1", "service": "extract_csv", "params": {"file_path": "/data/a.csv"}},
            {"id": "extract2", "service": "extract_csv", "params": {"file_path": "/data/b.csv"}},
            {"id": "join", "service": "join_datasets", "depends_on": ["extract1", "extract2"]},
        ])
        errors, warnings = validate_pipeline(pipeline, MINIMAL_REGISTRY)
        assert errors == []


# ── Return type contract ──────────────────────────────────────────────────────

class TestReturnTypeContract:
    def test_returns_tuple_of_two_lists(self):
        pipeline = _pipeline([
            {"id": "e", "service": "extract_csv", "params": {"file_path": "/data/f.csv"}},
            {"id": "s", "service": "load_data", "params": {"format": "csv"}, "depends_on": ["e"]},
        ])
        result = validate_pipeline(pipeline, MINIMAL_REGISTRY)
        assert isinstance(result, tuple)
        assert len(result) == 2
        errors, warnings = result
        assert isinstance(errors, list)
        assert isinstance(warnings, list)

    def test_dataset_name_param_ignored_in_required_check(self):
        """dataset_name is auto-injected by the compiler and must NOT trigger a missing-param error."""
        pipeline = _pipeline([
            {"id": "extract", "service": "extract_csv", "params": {"file_path": "/data/f.csv"}},
            {"id": "save", "service": "load_data", "params": {"format": "csv"}, "depends_on": ["extract"]},
        ])
        errors, warnings = validate_pipeline(pipeline, MINIMAL_REGISTRY)
        assert not any("dataset_name" in e for e in errors)
