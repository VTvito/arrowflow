"""
Pipeline Compiler & Executor.

Takes a validated pipeline definition and executes it step-by-step
using the Preparator SDK. Tracks execution state and collects metrics.
"""

import logging
import time
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger("ai_agent.pipeline_compiler")


@dataclass
class StepResult:
    """Result of a single pipeline step execution."""
    step_id: str
    service: str
    status: str  # "success", "error", "skipped"
    duration_sec: float = 0.0
    data_size_bytes: int = 0
    error_message: str = ""
    metadata: dict = field(default_factory=dict)


@dataclass
class PipelineResult:
    """Aggregated result of a full pipeline execution."""
    pipeline_name: str
    status: str  # "completed", "failed", "partial"
    total_duration_sec: float = 0.0
    steps: list = field(default_factory=list)
    correlation_id: str = ""

    def to_dict(self) -> dict:
        return {
            "pipeline_name": self.pipeline_name,
            "status": self.status,
            "total_duration_sec": round(self.total_duration_sec, 3),
            "correlation_id": self.correlation_id,
            "steps": [
                {
                    "step_id": s.step_id,
                    "service": s.service,
                    "status": s.status,
                    "duration_sec": round(s.duration_sec, 3),
                    "data_size_bytes": s.data_size_bytes,
                    "error_message": s.error_message,
                }
                for s in self.steps
            ],
        }


class PipelineCompiler:
    """Compiles and executes ETL pipeline definitions using the Preparator SDK."""

    def __init__(self, preparator):
        """
        Args:
            preparator: An instance of Preparator (v4).
        """
        self.prep = preparator
        self.logger = logging.getLogger("PipelineCompiler")
        self.last_step_outputs = {}  # step_id -> output data (available after execute())

    def execute(self, pipeline_def: dict, progress_callback=None) -> PipelineResult:
        """
        Execute a pipeline definition step-by-step.

        Args:
            pipeline_def: Validated pipeline definition dict.
            progress_callback: Optional callable(step_id, status, progress_pct) for UI updates.

        Returns:
            PipelineResult with execution details.
        """
        pipeline = pipeline_def["pipeline"]
        pipeline_name = pipeline["name"]
        steps = pipeline["steps"]
        total_steps = len(steps)

        result = PipelineResult(
            pipeline_name=pipeline_name, status="running",
            correlation_id=getattr(self.prep, "correlation_id", ""),
        )
        step_outputs = {}  # step_id -> IPC bytes
        self.last_step_outputs = {}
        start_time = time.time()

        self.logger.info(
            f"Executing pipeline '{pipeline_name}' with {total_steps} steps "
            f"[correlation_id={result.correlation_id}]"
        )

        for i, step in enumerate(steps):
            step_id = step["id"]
            service = step["service"]
            params = step.get("params", {})
            depends_on = step.get("depends_on", [])

            if progress_callback:
                progress_callback(step_id, "running", (i / total_steps) * 100)

            self.logger.info(f"Step [{i+1}/{total_steps}] {step_id}: {service}")
            step_start = time.time()

            try:
                # Resolve input data from dependencies
                input_data = None
                if depends_on:
                    # Use the output of the last dependency
                    input_data = step_outputs.get(depends_on[-1])

                # Special handling for join_datasets (needs two inputs)
                if service == "join_datasets" and len(depends_on) >= 2:
                    input_data_2 = step_outputs.get(depends_on[1]) if len(depends_on) >= 2 else None
                    output_data = self._execute_step(
                        service, params, input_data, pipeline_name,
                        input_data_2=input_data_2,
                    )
                else:
                    # Execute the service call
                    output_data = self._execute_step(service, params, input_data, pipeline_name)

                step_result = StepResult(
                    step_id=step_id,
                    service=service,
                    status="success",
                    duration_sec=time.time() - step_start,
                    data_size_bytes=len(output_data) if isinstance(output_data, bytes) else 0,
                )
                step_outputs[step_id] = output_data
                self.last_step_outputs[step_id] = output_data

            except Exception as e:
                self.logger.error(f"Step '{step_id}' failed: {e}")
                step_result = StepResult(
                    step_id=step_id,
                    service=service,
                    status="error",
                    duration_sec=time.time() - step_start,
                    error_message=str(e),
                )
                result.steps.append(step_result)
                result.status = "failed"
                result.total_duration_sec = time.time() - start_time

                if progress_callback:
                    progress_callback(step_id, "error", (i / total_steps) * 100)
                break

            result.steps.append(step_result)
            if progress_callback:
                progress_callback(step_id, "success", ((i + 1) / total_steps) * 100)

        if result.status != "failed":
            result.status = "completed"

        result.total_duration_sec = time.time() - start_time
        self.logger.info(
            f"Pipeline '{pipeline_name}' {result.status} in {result.total_duration_sec:.2f}s"
        )
        return result

    def _execute_step(
        self, service: str, params: dict,
        input_data: Optional[bytes], dataset_name: str,
        input_data_2: Optional[bytes] = None,
    ) -> bytes:
        """Execute a single pipeline step and return the output data."""

        if service == "extract_csv":
            return self.prep.extract_csv(
                dataset_name=dataset_name,
                file_path=params["file_path"],
            )

        elif service == "extract_excel":
            return self.prep.extract_excel(
                dataset_name=dataset_name,
                file_path=params["file_path"],
            )

        elif service == "extract_api":
            return self.prep.extract_api(
                dataset_name=dataset_name,
                api_url=params["api_url"],
                api_params=params.get("api_params", {}),
                auth_type=params.get("auth_type"),
                auth_value=params.get("auth_value"),
            )

        elif service == "extract_sql":
            return self.prep.extract_sql(
                dataset_name=dataset_name,
                db_url=params["db_url"],
                query=params["query"],
            )

        elif service == "clean_nan":
            return self.prep.clean_nan(
                input_data,
                dataset_name=dataset_name,
                strategy=params.get("strategy", "drop"),
                fill_value=params.get("fill_value"),
                columns=params.get("columns"),
            )

        elif service == "delete_columns":
            return self.prep.delete_columns(
                input_data,
                columns=params["columns"],
                dataset_name=dataset_name,
            )

        elif service == "data_quality":
            return self.prep.check_quality(
                input_data,
                dataset_name=dataset_name,
                rules=params.get("rules", {}),
                fail_on_errors=params.get("fail_on_errors", False),
            )

        elif service == "outlier_detection":
            return self.prep.detect_outliers(
                input_data,
                dataset_name=dataset_name,
                column=params["column"],
                z_threshold=params.get("z_threshold", 3.0),
            )

        elif service == "join_datasets":
            if input_data is None or input_data_2 is None:
                raise ValueError(
                    "join_datasets requires exactly 2 depends_on entries "
                    "(one for each input dataset)"
                )
            return self.prep.join_datasets(
                input_data,
                input_data_2,
                dataset_name=dataset_name,
                join_key=params.get("join_key", "id"),
                join_type=params.get("join_type", "inner"),
            )

        elif service == "text_completion_llm":
            return self.prep.text_completion_llm(
                input_data,
                dataset_name=dataset_name,
                text_column=params["text_column"],
                max_tokens=params.get("max_tokens", 20),
                missing_placeholder=params.get("missing_placeholder", "[MISSING]"),
            )

        elif service == "load_data":
            return self.prep.load_data(
                input_data,
                format=params.get("format", "csv"),
                dataset_name=dataset_name,
            )

        else:
            raise ValueError(f"Unknown service: {service}")
