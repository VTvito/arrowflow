"""
Pipeline Compiler & Executor.

Takes a validated pipeline definition and executes it using the Preparator SDK.
Supports **parallel execution** of independent steps via topological layering
and a **dispatch registry** for extensibility without code changes.

Architecture:
    1. Build a DAG from ``depends_on`` edges.
    2. Topologically sort steps into *layers* — steps within a layer have no
       mutual dependencies and can run concurrently.
    3. Execute each layer with ``concurrent.futures.ThreadPoolExecutor``.
    4. Dispatch each service call through a registry dict (no if/elif chain).
"""

import logging
import time
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Callable, Optional

logger = logging.getLogger("ai_agent.pipeline_compiler")


# ── Data classes ────────────────────────────────────────────────────

@dataclass
class StepResult:
    """Result of a single pipeline step execution."""
    step_id: str
    service: str
    status: str  # "success", "error", "skipped"
    duration_sec: float = 0.0
    data_size_bytes: int = 0
    error_message: str = ""


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


# ── Topological helpers ─────────────────────────────────────────────

def _topological_layers(steps: list[dict]) -> list[list[dict]]:
    """
    Partition pipeline steps into execution layers using Kahn's algorithm.

    Steps within the same layer have no dependency on each other and can
    be executed in parallel.  Layers are returned in execution order.

    Returns:
        List of layers, where each layer is a list of step dicts.

    Raises:
        ValueError: If the dependency graph contains a cycle.
    """
    id_to_step: dict[str, dict] = {s["id"]: s for s in steps}
    in_degree: dict[str, int] = {s["id"]: 0 for s in steps}
    children: dict[str, list[str]] = defaultdict(list)

    for step in steps:
        for dep in step.get("depends_on", []):
            children[dep].append(step["id"])
            in_degree[step["id"]] += 1

    layers: list[list[dict]] = []
    queue = deque(sid for sid, deg in in_degree.items() if deg == 0)
    visited = 0

    while queue:
        layer: list[dict] = []
        next_queue: deque[str] = deque()
        while queue:
            sid = queue.popleft()
            layer.append(id_to_step[sid])
            visited += 1
            for child in children[sid]:
                in_degree[child] -= 1
                if in_degree[child] == 0:
                    next_queue.append(child)
        layers.append(layer)
        queue = next_queue

    if visited != len(steps):
        raise ValueError("Pipeline dependency graph contains a cycle")

    return layers


# ── Service dispatch registry ──────────────────────────────────────

def _build_dispatch_registry(prep) -> dict[str, Callable]:
    """
    Build a mapping of service_name → callable(params, input_data, dataset_name, input_data_2).

    Adding a new service only requires adding one entry here — no if/elif needed.
    Each callable returns the output bytes from the Preparator SDK method.
    """
    return {
        "extract_csv": lambda p, _inp, ds, _inp2: prep.extract_csv(
            dataset_name=ds,
            file_path=p["file_path"],
        ),
        "extract_excel": lambda p, _inp, ds, _inp2: prep.extract_excel(
            dataset_name=ds,
            file_path=p["file_path"],
        ),
        "extract_api": lambda p, _inp, ds, _inp2: prep.extract_api(
            dataset_name=ds,
            api_url=p["api_url"],
            api_params=p.get("api_params", {}),
            auth_type=p.get("auth_type"),
            auth_value=p.get("auth_value"),
        ),
        "extract_sql": lambda p, _inp, ds, _inp2: prep.extract_sql(
            dataset_name=ds,
            db_url=p["db_url"],
            query=p["query"],
        ),
        "clean_nan": lambda p, inp, ds, _inp2: prep.clean_nan(
            inp,
            dataset_name=ds,
            strategy=p.get("strategy", "drop"),
            fill_value=p.get("fill_value"),
            columns=p.get("columns"),
        ),
        "delete_columns": lambda p, inp, ds, _inp2: prep.delete_columns(
            inp,
            columns=p["columns"],
            dataset_name=ds,
        ),
        "data_quality": lambda p, inp, ds, _inp2: prep.check_quality(
            inp,
            dataset_name=ds,
            rules=p.get("rules", {}),
            fail_on_errors=p.get("fail_on_errors", False),
        ),
        "outlier_detection": lambda p, inp, ds, _inp2: prep.detect_outliers(
            inp,
            dataset_name=ds,
            column=p["column"],
            z_threshold=p.get("z_threshold", 3.0),
        ),
        "join_datasets": lambda p, inp, ds, inp2: prep.join_datasets(
            inp,
            inp2,
            dataset_name=ds,
            join_key=p.get("join_key", "id"),
            join_type=p.get("join_type", "inner"),
        ),
        "text_completion_llm": lambda p, inp, ds, _inp2: prep.text_completion_llm(
            inp,
            dataset_name=ds,
            text_column=p["text_column"],
            max_tokens=p.get("max_tokens", 20),
            missing_placeholder=p.get("missing_placeholder", "[MISSING]"),
        ),
        "load_data": lambda p, inp, ds, _inp2: prep.load_data(
            inp,
            format=p.get("format", "csv"),
            dataset_name=ds,
        ),
    }


_EXTRACT_SERVICES = {"extract_csv", "extract_excel", "extract_api", "extract_sql"}


# ── Pipeline Compiler ──────────────────────────────────────────────

class PipelineCompiler:
    """
    Compiles and executes ETL pipeline definitions using the Preparator SDK.

    Features:
        - **Dispatch registry**: service → callable mapping; add new services
          without touching the executor logic.
        - **Parallel execution**: independent steps (no mutual ``depends_on``)
          run concurrently via ``ThreadPoolExecutor``.
        - **Progress callbacks**: for UI integration (Streamlit, etc.).
    """

    def __init__(self, preparator, max_workers: int = 4):
        """
        Args:
            preparator: An instance of Preparator (v4).
            max_workers: Max threads for parallel step execution (default: 4).
        """
        self.prep = preparator
        self.last_step_outputs: dict[str, bytes] = {}
        self.max_workers = max_workers
        self._dispatch = _build_dispatch_registry(preparator)

    def register_service(self, service_name: str, handler: Callable) -> None:
        """
        Register a custom service handler at runtime.

        Args:
            service_name: Service key (must match pipeline YAML ``service`` field).
            handler: Callable with signature (params, input_data, dataset_name, input_data_2) -> bytes.
        """
        self._dispatch[service_name] = handler
        logger.info(f"Registered custom service handler: {service_name}")

    # ── Public API ──────────────────────────────────────────────────

    def execute(self, pipeline_def: dict, progress_callback=None) -> PipelineResult:
        """
        Execute a pipeline definition with parallel step support.

        Steps are grouped into topological layers.  Steps within a layer
        have no mutual dependencies and execute concurrently.
        Steps across layers execute sequentially (respecting data flow).

        Args:
            pipeline_def: Validated pipeline definition dict.
            progress_callback: Optional callable(step_id, status, progress_pct)
                               for UI updates.

        Returns:
            PipelineResult with execution details.
        """
        pipeline = pipeline_def["pipeline"]
        pipeline_name = pipeline["name"]
        steps = pipeline["steps"]
        total_steps = len(steps)

        result = PipelineResult(
            pipeline_name=pipeline_name,
            status="running",
            correlation_id=getattr(self.prep, "correlation_id", ""),
        )
        step_outputs: dict[str, bytes] = {}
        self.last_step_outputs = {}
        start_time = time.time()
        completed_count = 0

        # Build execution layers (topological sort)
        layers = _topological_layers(steps)
        parallel_layers = sum(1 for layer in layers if len(layer) > 1)

        logger.info(
            f"Executing pipeline '{pipeline_name}' — {total_steps} steps in "
            f"{len(layers)} layers ({parallel_layers} parallel) "
            f"[correlation_id={result.correlation_id}]"
        )

        failed = False

        for layer_idx, layer in enumerate(layers):
            if failed:
                break

            if len(layer) == 1:
                # Single step — execute directly (no thread overhead)
                step = layer[0]
                step_result = self._run_single_step(
                    step, step_outputs, pipeline_name, completed_count, total_steps, progress_callback,
                )
                result.steps.append(step_result)

                if step_result.status == "error":
                    result.status = "failed"
                    failed = True
                else:
                    step_outputs[step["id"]] = self.last_step_outputs[step["id"]]
                    completed_count += 1
            else:
                # Multiple independent steps — execute in parallel
                logger.info(
                    f"Layer {layer_idx + 1}: running {len(layer)} steps in parallel "
                    f"[{', '.join(s['id'] for s in layer)}]"
                )
                layer_results = self._run_parallel_layer(
                    layer, step_outputs, pipeline_name, completed_count, total_steps, progress_callback,
                )
                for sr in layer_results:
                    result.steps.append(sr)
                    if sr.status == "error":
                        result.status = "failed"
                        failed = True
                    else:
                        step_outputs[sr.step_id] = self.last_step_outputs[sr.step_id]
                        completed_count += 1

        if not failed:
            result.status = "completed"

        result.total_duration_sec = time.time() - start_time
        logger.info(
            f"Pipeline '{pipeline_name}' {result.status} in {result.total_duration_sec:.2f}s"
        )
        return result

    # ── Internal execution ──────────────────────────────────────────

    def _run_single_step(
        self, step: dict, step_outputs: dict, pipeline_name: str,
        completed_count: int, total_steps: int, progress_callback,
    ) -> StepResult:
        """Execute a single step and return its StepResult."""
        step_id = step["id"]
        service = step["service"]
        params = step.get("params", {})
        depends_on = step.get("depends_on", [])

        if progress_callback:
            progress_callback(step_id, "running", (completed_count / total_steps) * 100)

        logger.info(f"Step [{completed_count + 1}/{total_steps}] {step_id}: {service}")
        step_start = time.time()

        try:
            input_data, input_data_2 = self._resolve_inputs(depends_on, step_outputs, service)
            output_data = self._dispatch_step(service, params, input_data, pipeline_name, input_data_2)

            self.last_step_outputs[step_id] = output_data
            sr = StepResult(
                step_id=step_id,
                service=service,
                status="success",
                duration_sec=time.time() - step_start,
                data_size_bytes=len(output_data) if isinstance(output_data, bytes) else 0,
            )
            if progress_callback:
                progress_callback(step_id, "success", ((completed_count + 1) / total_steps) * 100)
            return sr

        except Exception as e:
            logger.error(f"Step '{step_id}' failed: {e}")
            if progress_callback:
                progress_callback(step_id, "error", (completed_count / total_steps) * 100)
            return StepResult(
                step_id=step_id,
                service=service,
                status="error",
                duration_sec=time.time() - step_start,
                error_message=str(e),
            )

    def _run_parallel_layer(
        self, layer: list[dict], step_outputs: dict, pipeline_name: str,
        completed_count: int, total_steps: int, progress_callback,
    ) -> list[StepResult]:
        """Execute all steps in a layer concurrently and return results."""
        results: list[StepResult] = []
        workers = min(self.max_workers, len(layer))

        with ThreadPoolExecutor(max_workers=workers) as pool:
            future_to_step = {
                pool.submit(
                    self._run_single_step,
                    step, step_outputs, pipeline_name,
                    completed_count + i, total_steps, progress_callback,
                ): step
                for i, step in enumerate(layer)
            }
            for future in as_completed(future_to_step):
                results.append(future.result())

        return results

    def _resolve_inputs(
        self, depends_on: list[str], step_outputs: dict, service: str,
    ) -> tuple[Optional[bytes], Optional[bytes]]:
        """Resolve primary and secondary input data from dependency outputs."""
        input_data: Optional[bytes] = None
        input_data_2: Optional[bytes] = None

        if depends_on:
            input_data = step_outputs.get(depends_on[0])
            if service == "join_datasets" and len(depends_on) >= 2:
                input_data_2 = step_outputs.get(depends_on[1])

        return input_data, input_data_2

    def _dispatch_step(
        self, service: str, params: dict,
        input_data: Optional[bytes], dataset_name: str,
        input_data_2: Optional[bytes] = None,
    ) -> bytes:
        """Dispatch a step to the appropriate service handler via the registry."""
        handler = self._dispatch.get(service)
        if handler is None:
            raise ValueError(
                f"Unknown service: '{service}'. "
                f"Registered services: {sorted(self._dispatch.keys())}"
            )

        # Validate join_datasets inputs
        if service == "join_datasets" and (input_data is None or input_data_2 is None):
            raise ValueError(
                "join_datasets requires exactly 2 depends_on entries "
                "(one for each input dataset)"
            )

        if service not in _EXTRACT_SERVICES and input_data is None:
            raise ValueError(
                f"Service '{service}' requires input data. "
                "Check depends_on and upstream outputs."
            )

        return handler(params, input_data, dataset_name, input_data_2)
