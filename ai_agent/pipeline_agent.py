"""
Pipeline Agent — Translates natural language requests into ETL pipeline definitions.

Uses the LLM abstraction layer to generate pipeline YAML from user requests,
validates against the pipeline schema, and can explain generated pipelines.
"""

import json
import logging
import os

import yaml

logger = logging.getLogger("ai_agent.pipeline_agent")

# Load the service registry for the system prompt
SCHEMA_DIR = os.path.join(os.path.dirname(__file__), "..", "schemas")


def _load_service_registry() -> dict:
    registry_path = os.path.join(SCHEMA_DIR, "service_registry.json")
    with open(registry_path) as f:
        return json.load(f)


def _load_pipeline_schema() -> dict:
    schema_path = os.path.join(SCHEMA_DIR, "pipeline_schema.json")
    with open(schema_path) as f:
        return json.load(f)


def _build_system_prompt() -> str:
    """Build the system prompt that instructs the LLM to generate pipeline YAML."""
    registry = _load_service_registry()

    services_desc = []
    for key, svc in registry["services"].items():
        params_desc = []
        for pname, pinfo in svc.get("params", {}).items():
            req = "required" if pinfo.get("required") else "optional"
            params_desc.append(f"      - {pname} ({pinfo['type']}, {req}): {pinfo['description']}")
        params_text = "\n".join(params_desc) if params_desc else "      (no parameters)"
        services_desc.append(
            f"  - {key}: {svc['description']}\n"
            f"    Type: {svc['type']}\n"
            f"    Parameters:\n{params_text}"
        )

    services_text = "\n\n".join(services_desc)

    return f"""You are an ETL Pipeline Architect. You generate YAML pipeline definitions \
for an ETL microservices platform.

AVAILABLE SERVICES:
{services_text}

RULES:
1. Every pipeline MUST start with an extract step (extract_csv, extract_excel, extract_api, or extract_sql).
2. Every pipeline SHOULD end with a load_data step to save results.
3. Transform steps (clean_nan, delete_columns, data_quality, outlier_detection,
   text_completion_llm) go between extract and load.
4. Each step must have a unique 'id' and reference the 'service' name exactly as listed above.
5. Use 'depends_on' to define the execution order. Each step (except the first) must depend on at least one prior step.
6. The 'params' object must contain all required parameters for the service.
7. The pipeline 'name' must be a valid identifier (letters, numbers, underscores, hyphens only).
8. dataset_name is automatically set to the pipeline name — do NOT include it in step params.

OUTPUT FORMAT:
Return ONLY valid YAML, no markdown fences, no explanations. The YAML must follow this structure:

pipeline:
  name: my_pipeline
  description: Brief description
  steps:
    - id: step1
      service: extract_csv
      params:
        file_path: /app/data/myfile.csv
    - id: step2
      service: clean_nan
      depends_on: [step1]
    - id: step3
      service: load_data
      params:
        format: csv
      depends_on: [step2]
"""


def validate_pipeline(pipeline_def: dict, registry: dict) -> tuple[list[str], list[str]]:
    """
    Validate a pipeline definition against the service registry.

    This is a standalone function so it can be used without instantiating PipelineAgent
    (e.g., in the Streamlit YAML editor without an LLM provider).

    Args:
        pipeline_def: Parsed pipeline definition dict.
        registry: Service registry dict (from service_registry.json).

    Returns:
        Tuple of (errors, warnings).
        Errors block execution; warnings are informational and do not block.
    """
    errors: list[str] = []
    warnings: list[str] = []

    if not pipeline_def or "pipeline" not in pipeline_def:
        return ["Missing top-level 'pipeline' key"], []

    pipeline = pipeline_def["pipeline"]

    if "name" not in pipeline:
        errors.append("Missing 'name' field")

    if "steps" not in pipeline or not pipeline["steps"]:
        errors.append("Missing or empty 'steps' list")
        return errors, warnings

    step_ids: set[str] = set()
    valid_services = set(registry["services"].keys())
    has_extract = False
    has_load = False

    for i, step in enumerate(pipeline["steps"]):
        step_id = step.get("id", f"step_{i}")

        if step_id in step_ids:
            errors.append(f"Duplicate step ID: '{step_id}'")
        step_ids.add(step_id)

        service = step.get("service")
        if not service:
            errors.append(f"Step '{step_id}': missing 'service' field")
        elif service not in valid_services:
            errors.append(
                f"Step '{step_id}': unknown service '{service}'. Valid: {sorted(valid_services)}"
            )
        else:
            svc_info = registry["services"][service]
            if svc_info["type"] == "extract":
                has_extract = True
            if service == "load_data":
                has_load = True

            # Check required params
            for pname, pinfo in svc_info.get("params", {}).items():
                if pname == "dataset_name":
                    continue  # auto-injected by the compiler
                if pinfo.get("required") and pname not in step.get("params", {}):
                    errors.append(
                        f"Step '{step_id}': missing required param '{pname}' for service '{service}'"
                    )

        # Validate depends_on references
        for dep in step.get("depends_on", []):
            if dep not in step_ids:
                errors.append(f"Step '{step_id}': depends_on references unknown step '{dep}'")

    if not has_extract:
        errors.append("Pipeline must have at least one extract step")

    if not has_load:
        warnings.append("Pipeline has no load_data step — results will not be saved to disk")

    return errors, warnings


class PipelineAgent:
    """Agent that generates and validates ETL pipeline definitions from natural language."""

    def __init__(self, llm_provider):
        """
        Args:
            llm_provider: An instance of LLMProvider (OpenAI or Local).
        """
        self.llm = llm_provider
        self.system_prompt = _build_system_prompt()
        self.schema = _load_pipeline_schema()
        self.registry = _load_service_registry()
        logger.info(f"Pipeline agent initialized with LLM: {llm_provider.name()}")

    def generate_pipeline(self, user_request: str) -> dict:
        """
        Generate a pipeline definition from a natural language request.

        Args:
            user_request: Natural language description of the desired ETL pipeline.

        Returns:
            Parsed pipeline definition dict.

        Raises:
            ValueError: If the generated YAML is invalid.
        """
        logger.info(f"Generating pipeline from request: {user_request[:100]}...")

        raw_output = self.llm.generate(
            prompt=user_request,
            system_prompt=self.system_prompt,
            temperature=0.2,
            max_tokens=2048,
        )

        # Clean up potential markdown fences
        yaml_text = raw_output.strip()
        if yaml_text.startswith("```"):
            lines = yaml_text.split("\n")
            yaml_text = "\n".join(lines[1:-1]) if lines[-1].strip() == "```" else "\n".join(lines[1:])

        try:
            pipeline_def = yaml.safe_load(yaml_text)
        except yaml.YAMLError as e:
            raise ValueError(f"LLM generated invalid YAML: {e}\n\nRaw output:\n{raw_output}")

        # Validate structure — only errors block execution, warnings are logged
        errors, warnings = self.validate_pipeline(pipeline_def)
        for w in warnings:
            logger.warning(f"Pipeline warning: {w}")
        if errors:
            raise ValueError("Pipeline validation failed:\n" + "\n".join(f"  - {e}" for e in errors))

        pipeline_name = pipeline_def['pipeline']['name']
        step_count = len(pipeline_def['pipeline']['steps'])
        logger.info(f"Generated pipeline '{pipeline_name}' with {step_count} steps")
        return pipeline_def

    def validate_pipeline(self, pipeline_def: dict) -> tuple[list[str], list[str]]:
        """
        Validate a pipeline definition against the schema and service registry.

        Thin wrapper around the module-level ``validate_pipeline`` function.

        Returns:
            Tuple of (errors, warnings).
            Errors block execution; warnings are informational and do not block.
        """
        return validate_pipeline(pipeline_def, self.registry)

    def explain_pipeline(self, pipeline_def: dict) -> str:
        """
        Generate a human-readable explanation of a pipeline.

        Args:
            pipeline_def: Parsed pipeline definition.

        Returns:
            Natural language explanation.
        """
        pipeline = pipeline_def["pipeline"]
        lines = [f"**Pipeline: {pipeline['name']}**"]

        if pipeline.get("description"):
            lines.append(f"_{pipeline['description']}_")

        lines.append(f"\n**Steps ({len(pipeline['steps'])}):**")

        for i, step in enumerate(pipeline["steps"], 1):
            service = step["service"]
            svc_info = self.registry["services"].get(service, {})
            svc_name = svc_info.get("name", service)
            params = step.get("params", {})
            params_str = ", ".join(f"{k}={v}" for k, v in params.items()) if params else "default settings"
            deps = step.get("depends_on", [])
            dep_str = f" (after: {', '.join(deps)})" if deps else ""

            lines.append(f"{i}. **{svc_name}** [{step['id']}]{dep_str} — {params_str}")

        return "\n".join(lines)
