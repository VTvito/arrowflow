# Copilot Instructions — ETL Microservices Platform

> These instructions provide context for AI agents (GitHub Copilot, Copilot Chat, agentic workflows) working on this codebase. They describe architecture, conventions, patterns, constraints, and lessons learned so that an AI agent can make correct decisions without re-discovering project structure.

---

## 1. Project Identity

**What this is:** An AI-assisted, modular ETL (Extract, Transform, Load) platform where each data operation is an independent Flask microservice. Pipelines are orchestrated via Apache Airflow DAGs or via the AI agent (natural language → YAML → execution).

**Primary use case:** HR / People Analytics and E-commerce — the platform ships with production-ready pipelines for the IBM HR Attrition dataset and e-commerce order analytics, plus a weather API demo. Bundled demo datasets in `data/demo/` allow out-of-the-box testing.

**Core differentiator:** Composable, dynamically-assembled pipelines where an AI agent can translate natural language into validated YAML pipeline definitions and execute them. Each microservice is independently deployable, observable, and scalable.

---

## 2. Architecture Overview

```
[Data Source] → [Extract Service] →─── Arrow IPC ───→ [Transform Service(s)] →─── Arrow IPC ───→ [Load Service] → [Output]
                                                            ↑
                                   Airflow DAG or AI Agent orchestrates via Preparator SDK
```

### Communication Patterns

| Pattern | Services | Request Body | Parameters | Response |
|---|---|---|---|---|
| **JSON → Arrow IPC** | Extract (CSV, SQL, API, Excel) | JSON config | In JSON body | Arrow IPC binary |
| **Arrow IPC → Arrow IPC** | Transform (clean, delete, outliers, quality, LLM) | Arrow IPC binary | `X-Params` HTTP header (JSON string) | Arrow IPC binary |
| **Arrow IPC → File** | Load | Arrow IPC binary | `X-Params` header | JSON status |
| **Multipart → Arrow IPC** | Join | Two Arrow IPC files as form fields | `X-Params` header | Arrow IPC binary |

**Critical invariant:** All inter-service data transfer uses Apache Arrow IPC streaming format. Never CSV/JSON between services.

### The Preparator SDK (`preparator/preparator_v4.py`)

The central client-side orchestration layer. All pipeline steps (DAGs, AI agent, Streamlit UI) call microservices through this SDK.

Key capabilities:
- `requests.Session()` with connection pooling
- Retry strategy: 3 retries, 0.5s exponential backoff, on HTTP 502/503/504
- Configurable timeout: `(connect=5s, read=300s)` default
- Context manager: `with Preparator(config) as prep:`
- Structured error handling: parses JSON error bodies from services

Methods mirror services: `extract_csv()`, `extract_sql()`, `extract_api()`, `extract_excel()`, `clean_nan(strategy, fill_value, columns)`, `delete_columns()`, `detect_outliers()`, `check_quality()`, `join_datasets()`, `text_completion_llm()`, `load_data()`.

- **Correlation ID:** Every Preparator instance generates a UUID `correlation_id` (or accepts one). All HTTP requests include `X-Correlation-ID` header for end-to-end tracing.

---

## 3. Services Catalog

All services run on the `etl-network` Docker bridge, referenced by container name. Endpoints registered in `preparator/services_config.json`.

| Service | Port | Key | Type | Logic Module |
|---|---|---|---|---|
| `extract-csv-service` | 5001 | `extract_csv` | extract | `read.py` |
| `clean-nan-service` | 5002 | `clean_nan` | transform | `clean.py` |

The clean-nan service supports 7 strategies via the `strategy` parameter: `drop` (default), `fill_mean`, `fill_median`, `fill_mode`, `fill_value`, `ffill`, `bfill`. Optional `columns` parameter scopes the strategy to specific columns. Optional `fill_value` parameter is required when `strategy=fill_value`.

| `delete-columns-service` | 5004 | `delete_columns` | transform | `columns.py` |
| `extract-sql-service` | 5005 | `extract_sql` | extract | `extract.py` |
| `extract-api-service` | 5006 | `extract_api` | extract | `extract.py` |
| `extract-excel-service` | 5007 | `extract_excel` | extract | `extract.py` |
| `join-datasets-service` | 5008 | `join_datasets` | transform | `join.py` |
| `load-data-service` | 5009 | `load_data` | load | `load.py` |

The load-data service supports 4 output formats: `csv`, `xlsx`/`xls`, `json`, `parquet`. Parquet output is written directly from Arrow without Pandas conversion (uses `pyarrow.parquet.write_table` with snappy compression).

| `data-quality-service` | 5010 | `data_quality` | transform | `dq.py` |

The data-quality service supports 7 check types via the `rules` parameter: `min_rows`, `check_null_ratio`+`threshold_null_ratio`, `check_duplicates`, `check_column_types` (numeric/string/datetime), `check_unique_columns`, `check_value_range` (min/max per column), `check_completeness` (per-column completeness report).

| `outlier-detection-service` | 5011 | `outlier_detection` | transform | `outliers.py` |
| `text-completion-llm-service` | 5012 | `text_completion_llm` | transform | `completion.py` |
| `streamlit-app` | 8501 | — | ui | `streamlit_app/app.py` |

**Port gap:** Port 5003 is unused (was never assigned). Next available port: **5013**.

Every service exposes:
- `POST /<endpoint>` — main ETL operation
- `GET /health` — enriched health check (service identity, uptime, shared volume status, disk space)
- `GET /metrics` — Prometheus scrape endpoint (3 counters: requests_total, success_total, error_total)

All services propagate `X-Correlation-ID` header for end-to-end request tracing.

---

## 4. Project Structure

```
etl_microservices/
├── docker-compose.yml              # Full stack: 11 services + postgres + airflow + prometheus + grafana + streamlit
├── Makefile                        # Common commands: up, down, build, test, lint, benchmark
├── pyproject.toml                  # Project metadata, pytest/ruff/coverage config
├── README.md                       # Project overview and quickstart
├── .env                            # Environment variables (not committed)
├── .env.example                    # Template for .env
│
├── services/
│   ├── common/                     # Shared utilities (imported by all services)
│   │   ├── arrow_utils.py          # ipc_to_table(), table_to_ipc()
│   │   ├── json_utils.py           # NpEncoder (handles numpy types in JSON)
│   │   ├── path_utils.py           # sanitize_dataset_name(), resolve_input_path(), ensure_dataset_dirs() — security & paths
│   │   ├── logging_config.py       # JSONFormatter, CorrelationAdapter, configure_service_logging()
│   │   ├── health.py               # create_health_response() — enriched health checks
│   │   └── service_utils.py        # Counters, /health+/metrics, X-Params parsing, metadata, correlation ID
│   └── <service-name>/
│       ├── Dockerfile              # python:3.9-slim, gunicorn, HEALTHCHECK
│       ├── requirements.txt        # Always: flask, pyarrow, prometheus_client, pandas, numpy, gunicorn
│       ├── run.py                  # Entry point (only for local dev; gunicorn in Docker)
│       └── app/
│           ├── __init__.py         # Flask app factory: create_app()
│           ├── routes.py           # Blueprint, HTTP endpoints, Prometheus counters
│           └── <logic>.py          # Pure business logic (no Flask deps)
│
├── preparator/
│   ├── preparator_v4.py            # Client SDK (retry, timeout, context manager)
│   └── services_config.json        # Service key → URL mapping
│
├── airflow/
│   ├── Dockerfile                  # Airflow webserver + scheduler, db migrate on startup
│   └── dags/
│       ├── hr_analytics_pipeline.py                     # HR use case (6-step pipeline)
│       ├── ecommerce_pipeline.py                        # E-commerce order analytics pipeline
│       ├── weather_api_pipeline.py                      # Weather API extraction demo
│       ├── parametrized_preparator_v4_quality.py        # Generic quality pipeline
│       ├── parametrized_preparator_v4_ia.py             # Pipeline with LLM text completion
│       ├── parametrized_preparator_v4_quality_join.py   # Pipeline with dataset join
│       └── xcom_file_utils.py                           # File-based XCom (shared filesystem)
│
├── ai_agent/
│   ├── __init__.py
│   ├── llm_provider.py            # Abstract LLMProvider + OpenAIProvider + LocalProvider
│   ├── pipeline_agent.py          # NL → YAML pipeline generation + validation
│   └── pipeline_compiler.py       # Parallel pipeline execution via Preparator SDK (dispatch registry + topological layering)
│
├── streamlit_app/
│   ├── app.py                     # Streamlit UI: chat, YAML editor, execution monitor, service catalog, data preview/download, health dashboard
│   ├── Dockerfile
│   └── requirements.txt
│
├── schemas/
│   ├── pipeline_schema.json       # JSON Schema for pipeline definitions
│   └── service_registry.json      # Service discovery metadata (params, types, descriptions)
│
├── benchmark/
│   ├── generate_hr_dataset.py     # Synthetic HR data generator (1k–500k rows)
│   ├── monolithic_pipeline.py     # Pure Pandas baseline (same 6 steps)
│   ├── run_benchmark.py           # Runner with charts (matplotlib + plotly)
│   └── __init__.py
│
├── data/
│   └── demo/                       # Bundled demo datasets for out-of-the-box testing
│       ├── hr_sample.csv           # IBM HR Attrition sample (501 rows)
│       └── ecommerce_orders.csv    # E-commerce orders sample (501 rows)
│
├── docs/
│   └── extending.md               # Developer guide: add new services & create pipelines
│
├── examples/
│   └── pipelines/                  # Ready-to-use YAML pipeline definitions
│       ├── hr_analytics.yaml
│       ├── ecommerce_analytics.yaml
│       ├── weather_data.yaml
│       └── README.md
│
├── templates/
│   └── new_service/                # Scaffolding template with placeholders for new services
│       ├── Dockerfile
│       ├── requirements.txt
│       ├── run.py
│       ├── README.md               # Step-by-step checklist
│       └── app/
│
├── tests/
│   ├── conftest.py                # Shared fixtures (sample Arrow tables, IPC data, services_config)
│   ├── unit/                      # 17 test files (208 tests total with integration)
│   └── integration/               # 2 test files (Flask test client + Preparator SDK)
│
├── .github/
│   ├── copilot-instructions.md    # THIS FILE — AI agent context
│   └── workflows/ci.yml           # GitHub Actions: lint → test-unit → test-integration → docker build
│
└── prometheus/
    └── prometheus.yml             # Scrape targets for all 11 services
```

---

## 5. Coding Conventions & Patterns

### Service Structure (mandatory for every microservice)

```
services/<name>/
├── Dockerfile          # FROM python:3.9-slim, COPY common/, gunicorn CMD, HEALTHCHECK
├── requirements.txt    # flask, pyarrow, prometheus_client, pandas, numpy, gunicorn
├── run.py              # Flask dev entry point (not used in Docker)
└── app/
    ├── __init__.py     # create_app() → registers blueprint
    ├── routes.py       # Blueprint + Prometheus counters + endpoint handler
    └── <logic>.py      # Pure function(s) — takes Arrow Table + params, returns Arrow Table
```

### Endpoint Pattern (transform services)

All boilerplate is handled by shared utilities from `common/service_utils.py`:

```python
from common.service_utils import (
    create_service_counters, register_standard_endpoints,
    parse_x_params, save_metadata, get_correlation_id,
)
from common.logging_config import get_correlation_logger

REQUEST_COUNTER, SUCCESS_COUNTER, ERROR_COUNTER = create_service_counters('service_slug')
register_standard_endpoints(bp, 'service-name')

@bp.route('/<name>', methods=['POST'])
def handler():
    start_time = time.time()
    correlation_id = get_correlation_id()
    try:
        REQUEST_COUNTER.inc()
        params = parse_x_params()          # Raises ValueError on malformed JSON
        dataset_name = params.get('dataset_name', 'default_dataset')
        logger = get_correlation_logger('service-name', correlation_id, dataset_name)
        ipc_data = request.get_data()
        table_in = ipc_to_table(ipc_data)
        result_table = business_logic_function(table_in, ...)
        out_ipc = table_to_ipc(result_table)
        save_metadata('service-name', dataset_name, extra_fields={...}, start_time=start_time)
        SUCCESS_COUNTER.inc()
        response = Response(out_ipc, mimetype="application/vnd.apache.arrow.stream")
        response.headers['X-Correlation-ID'] = correlation_id
        return response, 200
    except ValueError as e:
        ERROR_COUNTER.inc()
        return jsonify({"status": "error", "message": str(e)}), 400
    except Exception as e:
        ERROR_COUNTER.inc()
        return jsonify({"status": "error", "message": str(e)}), 500
```

### Business Logic Isolation (strict rule)

- `routes.py` handles HTTP concerns: parsing requests, serializing responses, metrics, metadata files.
- `<logic>.py` handles data transformation only: receives `pyarrow.Table` or `pd.DataFrame`, returns same.
- Logic modules **must not import Flask** — this enables unit testing without HTTP scaffolding.

### Metadata Pattern

Every service writes JSON metadata after processing:
- Path: `/app/data/<dataset_name>/metadata/metadata_<service>_<timestamp>.json`
- Timestamp: `%Y%m%dT%H%M%SZ` (UTC)
- Fields: service name, dataset name, row/column counts, duration, service-specific stats

### Error Response Contract

```json
{"status": "error", "message": "Human-readable error description"}
```
- `200` + Arrow IPC body (or JSON for load-data) on success
- `400` + JSON error for client errors (bad params, missing data)
- `500` + JSON error for server errors (processing failures)

### Shared Utilities (`services/common/`)

```python
# Arrow serialization
from common.arrow_utils import ipc_to_table, table_to_ipc

# JSON encoding for numpy types
from common.json_utils import NpEncoder

# Structured JSON logging
from common.logging_config import configure_service_logging, get_correlation_logger

# Enriched health checks (uptime, volume status, disk space)
from common.health import create_health_response

# Route boilerplate elimination
from common.service_utils import (
    create_service_counters,           # 3 Prometheus Counters
    register_standard_endpoints,       # /health + /metrics on a Blueprint
    get_correlation_id,                # Read/generate X-Correlation-ID
    parse_x_params,                    # Parse X-Params header (ValueError on bad JSON)
    save_metadata,                     # Write metadata JSON file
)

# Path security & dataset directory management
from common.path_utils import (
    sanitize_dataset_name,             # Validate/sanitize dataset name
    resolve_input_path,                # Resolve and validate input file paths
    ensure_dataset_dirs,               # Create dataset + metadata directories
)
```

### Flask App Factory Pattern (`__init__.py`)

Every service's `create_app()` now includes:
- `MAX_CONTENT_LENGTH = 500 MB` — request size limit
- `configure_service_logging(SERVICE_NAME)` — structured JSON logging
- JSON error handlers for `404` and `413` (instead of default HTML responses)

---

## 6. AI Agent Layer

### Components

| Module | Purpose |
|---|---|
| `ai_agent/llm_provider.py` | Abstract `LLMProvider` + `OpenAIProvider` (GPT-4o-mini default) + `LocalProvider` (calls text-completion-llm-service) |
| `ai_agent/pipeline_agent.py` | `PipelineAgent`: builds system prompt from `service_registry.json`, calls LLM to generate YAML, validates structure + services + params + dependencies. Standalone `validate_pipeline()` module-level function enables validation-only use without instantiating the agent (e.g., Streamlit UI). |
| `ai_agent/pipeline_compiler.py` | `PipelineCompiler`: executes validated pipeline definitions via Preparator SDK with **parallel execution** of independent steps (topological layering via Kahn’s algorithm + `ThreadPoolExecutor`). Uses a **dispatch registry** (`_build_dispatch_registry()`) for extensibility—add new services via `register_service()` without if/elif chains. Returns `PipelineResult` with per-step metrics + `correlation_id`. Supports `join_datasets` (2 `depends_on` entries). Exposes `last_step_outputs` dict for UI data preview. |
| `schemas/service_registry.json` | Complete metadata for all 11 services: name, type, description, endpoint, input/output formats, params with types/required/defaults/enums |
| `schemas/pipeline_schema.json` | JSON Schema for pipeline definitions |

### Pipeline Definition Format (YAML)

```yaml
pipeline:
  name: hr_analytics
  description: HR data quality pipeline
  steps:
    - id: extract
      service: extract_csv
      params:
        file_path: /app/data/hr_attrition/data.csv
    - id: quality
      service: data_quality
      params:
        rules:
          min_rows: 10
          check_null_ratio: true
          threshold_null_ratio: 0.5
          check_duplicates: true
          check_completeness: true
      depends_on: [extract]
    - id: clean
      service: clean_nan
      params:
        strategy: fill_median
        columns: [age, salary]
      depends_on: [quality]
    - id: save
      service: load_data
      params: {format: parquet}
      depends_on: [clean]
```

### LLM Provider Configuration

- `LLM_PROVIDER=openai` → uses OpenAI API (requires `OPENAI_API_KEY`)
- `LLM_PROVIDER=local` → uses the local HuggingFace text-completion-llm-service
- Factory: `create_llm_provider(provider=None)` reads env var if not specified

---

## 7. Airflow & Orchestration

### DAG Pattern

All DAGs use `preparator_v4.Preparator` as the SDK to call services. Each task function:
1. Opens `services_config.json`
2. Creates a `Preparator` (as context manager)
3. Calls service methods
4. Returns data reference (IPC bytes as hex or shared file path)

### File-Based XCom (`xcom_file_utils.py`)

For datasets >50k rows, Airflow's PostgreSQL-backed XCom becomes a bottleneck. The `xcom_file_utils` module provides:

```python
save_ipc_to_shared(ipc_data, dataset_name, step_name) → str  # returns file path
load_ipc_from_shared(file_path) → bytes
cleanup_xcom_files(dataset_name) → int  # returns count of removed files
```

Files stored at `/app/data/<dataset_name>/xcom/<step>_<timestamp>_<uuid>.arrow`. Toggle via DAG parameter `use_file_xcom` (default: True).

### Available DAGs

| DAG ID | Description |
|---|---|
| `hr_analytics_pipeline` | 6-step HR analytics (extract → quality → drop cols → outliers → clean → load) |
| `ecommerce_pipeline` | E-commerce order analytics (extract → quality → clean → outliers → load) |
| `weather_api_pipeline` | Weather API extraction demo (extract API → quality → load) |
| `parametrized_preparator_v4_quality` | Generic quality pipeline (extract → quality → outliers → clean → load) |
| `parametrized_preparator_v4_ia` | Pipeline with LLM text completion step |
| `parametrized_preparator_v4_quality_join` | Pipeline with two-dataset join |

---

## 8. Docker & Infrastructure

### Runtime Configuration

- **WSGI server:** All services use **gunicorn** (4 workers, 300s timeout; LLM service: 1 worker, 600s timeout)
- **Health checks:** Every Dockerfile includes `HEALTHCHECK --interval=30s CMD curl -f http://localhost:<port>/health || exit 1`
- **Base image:** `python:3.9-slim` for all services
- **Shared volume:** `etl-containers-shared-data` mounted at `/app/data` across all containers

### Docker Compose Services

| Category | Services |
|---|---|
| **Infrastructure** | postgres, statsd-exporter, prometheus, grafana |
| **Orchestration** | airflow (webserver + scheduler) |
| **ETL Services** | 11 microservices (ports 5001–5012) |
| **UI** | streamlit-app (port 8501) |

### Network

Single bridge network `etl-network`. Services reference each other by container name (e.g., `http://clean-nan-service:5002`).

### Volumes

| Volume | Mount | Purpose |
|---|---|---|
| `etl-containers-shared-data` | `/app/data` | Shared datasets, metadata, XCom files |
| `etl-data-airflow` | `/opt/airflow` | Airflow persistence |
| `etl-postgres-data` | PostgreSQL data dir | Airflow metadata DB |
| `etl-grafana-data` | Grafana data dir | Dashboards |
| `etl-prometheus-data` | Prometheus data dir | TSDB |

### Environment Variables

| Variable | Default | Description |
|---|---|---|
| `POSTGRES_USER` | `airflow` | PostgreSQL username |
| `POSTGRES_PASSWORD` | `change-me-strong-password` | PostgreSQL password |
| `POSTGRES_DB` | `airflow` | PostgreSQL database |
| `GF_SECURITY_ADMIN_PASSWORD` | `change-me-strong-password` | Grafana admin password |
| `ETL_DATA_ROOT` | `/app/data` | Base directory for datasets/metadata path resolution |
| `ALLOW_PRIVATE_API_URLS` | `false` | Allow private/local API targets in `extract-api` |
| `HF_MODELS_PATH` | `./hf_models` | Local HuggingFace model cache |
| `LLM_PROVIDER` | `openai` | AI agent provider (`openai` or `local`) |
| `OPENAI_API_KEY` | — | OpenAI API key |
| `OPENAI_MODEL` | `gpt-4o-mini` | OpenAI model name |

---

## 9. Testing

### Test Architecture

```
tests/
├── conftest.py           # Shared fixtures: sample_arrow_table, sample_ipc_data, services_config, etc.
├── unit/                 # Test pure business logic (no HTTP)
│   ├── test_arrow_utils.py
│   ├── test_clean.py          # 14 tests (drop + 7 fill strategies + columns filter + validation)
│   ├── test_columns.py
│   ├── test_dq.py             # 20 tests (min_rows, null_ratio, duplicates, types, unique, range, completeness)
│   ├── test_extract_api.py    # 7 tests (URL validation, auth, public/private API handling)
│   ├── test_extract_csv.py    # 6 tests (CSV loading, path resolution, error handling)
│   ├── test_extract_excel.py  # 6 tests (Excel loading, path resolution, error handling)
│   ├── test_extract_sql.py    # 18 tests (SQL validation, injection prevention, query execution)
│   ├── test_health.py         # 10 tests (health response structure, env vars, volume checks)
│   ├── test_join.py
│   ├── test_load.py           # 11 tests (csv, json, xlsx, parquet, roundtrip, xls normalization, edge cases)
│   ├── test_logging_config.py # 14 tests (JSON formatter, CorrelationAdapter, configure, no-dup handlers)
│   ├── test_outliers.py
│   ├── test_path_utils.py
│   ├── test_pipeline_agent.py    # 16 tests (YAML generation, validation, schema compliance)
│   ├── test_pipeline_compiler.py # 21 tests (dispatch, topological layers, parallel execution, error handling)
│   └── test_service_utils.py  # 11 tests (counters, correlation ID, parse_x_params, save_metadata)
└── integration/          # Test Flask endpoints + Preparator SDK
    ├── test_service_endpoints.py
    └── test_preparator.py
```

### Key Fixtures (conftest.py)

- `sample_arrow_table` — 5 rows, 5 columns (id, name, age, salary, department)
- `sample_arrow_table_with_nulls` — same with NaN values
- `sample_arrow_table_with_outliers` — 100 rows with 2 injected outliers
- `large_arrow_table` — 100k rows for performance testing
- `sample_ipc_data` / `sample_ipc_data_with_nulls` — serialized Arrow IPC bytes
- `services_config` — test service URL configuration fixture

### Important: sys.path Constraint

Each service has its own `app/` Python package. Because all services share the `app/` namespace, test files for different services **cannot coexist on the same `sys.path`**. Each test file adds its own service directory to `sys.path` individually. This is intentional and **cannot** be consolidated into `conftest.py` — if multiple service `app/` dirs were on the path, `from app.clean import ...` would collide with `from app.dq import ...`.

**Critical: `sys.modules` cleanup is mandatory.** Before inserting a service's path and importing its `app` module, each test file must clear the cached `app` package from `sys.modules` to prevent namespace collisions:

```python
# Clear cached 'app' package to avoid namespace collision between services
for _mod in list(sys.modules):
    if _mod == "app" or _mod.startswith("app."):
        del sys.modules[_mod]
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "services", "<service-name>"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "services"))
from app.<module> import <function>  # noqa: E402
```

**Integration tests also require Prometheus registry cleanup** to avoid `Duplicated timeseries` errors when re-importing service modules that register counters. See `_clear_app_modules()` in `test_service_endpoints.py`.

### Running Tests

```bash
make test              # All tests
make test-unit         # Unit only
make test-integration  # Integration only
make test-coverage     # With coverage report
make lint              # ruff linter
```

---

## 10. Adding a New Microservice (Step-by-Step)

A ready-to-copy scaffolding template is available in `templates/new_service/` with placeholder syntax. A comprehensive developer guide is in `docs/extending.md`.

1. **Create directory:** Copy `templates/new_service/` to `services/<name>/` and replace placeholders (`{{SERVICE_NAME}}`, `{{SERVICE_PORT}}`, etc.).
2. **Logic module:** Pure function — takes `pyarrow.Table` + params, returns `pyarrow.Table`. No Flask imports.
3. **Routes:** Follow the endpoint pattern (section 5). Include REQUEST/SUCCESS/ERROR Prometheus counters. Include `/health` endpoint.
4. **Dockerfile:** Copy `common/`, install deps, set `PYTHONPATH=/app/services`, use gunicorn CMD with HEALTHCHECK.
5. **requirements.txt:** flask, pyarrow, prometheus_client, pandas, numpy, gunicorn (minimum).
6. **Register:**
   - `preparator/services_config.json` — key → URL
   - `schemas/service_registry.json` — full metadata (name, type, params, description)
   - `docker-compose.yml` — new service block
   - `prometheus/prometheus.yml` — new scrape target
7. **Preparator method:** Add typed method to `preparator/preparator_v4.py`
8. **Tests:** Unit test for logic module in `tests/unit/`, integration test in `tests/integration/`
9. **Port:** Use next available (current max: 5012, gap at 5003 — use **5013**).

---

## 11. Benchmark System

### Architecture

The benchmark compares the same 6-step ETL pipeline executed two ways:

| Approach | Implementation | Measures |
|---|---|---|
| **Monolith** | `benchmark/monolithic_pipeline.py` — single-process Pandas | Time, memory (tracemalloc), throughput |
| **Microservices** | Via Preparator SDK → HTTP → services (Docker) | Time, throughput |

### Dataset Generator

`benchmark/generate_hr_dataset.py` generates synthetic HR datasets matching the IBM HR Attrition schema. Supports scales: 1k, 10k, 50k, 100k, 500k rows. Includes realistic salary distribution (log-normal), injected outliers (0.5%), and NaN values (2% per nullable column).

### Running

```bash
make benchmark-data    # Generate all dataset scales
make benchmark-mono    # Monolith benchmark
make benchmark-micro   # Microservices benchmark (services must be running)
make benchmark-all     # Full comparison with charts
```

Results saved to `benchmark/results/` (PNG charts + interactive Plotly HTML report + JSON raw data).

---

## 12. Lessons Learned & Design Decisions

These are hard-won insights from building and debugging the platform. They should inform all future changes.

### Architecture Lessons

1. **Arrow IPC is the right wire format.** It's dramatically faster than JSON/CSV for columnar data and supports zero-copy reads. The overhead of HTTP + Arrow IPC serialization is negligible compared to the flexibility of independent services.

2. **X-Params header pattern works well.** Encoding parameters in the `X-Params` HTTP header (as a JSON string) cleanly separates the data payload (body) from configuration (header). This is preferable to URL query strings (v3 pattern, now deprecated) because it supports complex nested objects and avoids URL length limits.

3. **Business logic isolation is essential.** Keeping Flask/HTTP code in `routes.py` and pure data logic in a separate module makes the codebase testable, readable, and composable. Every service where this boundary was blurred was harder to test and debug.

4. **File-based XCom is mandatory for scale.** Airflow's PostgreSQL-backed XCom cannot handle binary payloads >1MB efficiently. For datasets >50k rows, passing the Arrow IPC file path (stored on the shared volume) instead of the data itself eliminates the bottleneck entirely.

### Bug Patterns to Watch For

5. **Mutable default arguments.** Python's `def func(params={})` shares the same dict across calls. Always use `def func(params=None)` + `params = params or {}`. This caused subtle data corruption in the Preparator SDK.

6. **Double Pandas conversion.** The naive pattern `table → pandas → process → table` in `routes.py`, followed by *another* `table → pandas` in `routes.py` for stats, doubles memory usage and processing time. Always compute stats inside the logic module and return them alongside the result.

7. **Prometheus /metrics inflating counters.** If the `/metrics` endpoint handler increments `REQUEST_COUNTER`, every Prometheus scrape (every 15s) inflates the count. The `/metrics` endpoint must **never** increment business counters.

8. **Column parsing type inconsistency.** The `delete_columns` service receives columns as either a list (from Preparator v4) or a comma-separated string (from manual calls). Always handle both: `isinstance(columns, list)` check first.

9. **Optional auth parameters.** The `extract_api` service originally required `auth_type` even for public APIs. Optional parameters must always have `None` defaults and explicit `if param is not None:` guards.

### Performance Lessons

10. **Gunicorn with 4 workers** is the minimum for production. The Flask dev server is single-threaded and blocks on I/O. Exception: the LLM service uses 1 worker (model memory is too large to replicate).

11. **Lazy model loading** for the LLM service. Loading the HuggingFace model at import time blocks container startup for 30+ seconds. Use a `get_model()` function with a module-level `_model = None` cache.

12. **The services approach has fixed HTTP overhead** (~5–15ms per hop). For small datasets (<1k rows), the monolith is significantly faster. The crossover point where microservices overhead becomes negligible is around 50k+ rows, where the actual data processing dominates.

### Testing Lessons

13. **Each service's `app/` package is a namespace collision.** All 11 services have an `app/` directory. Python resolves `from app.clean import ...` using the first `app/` on `sys.path`. Test files must add only their specific service dir to `sys.path`, never all services at once. Each test must clear `sys.modules` of all `app.*` entries before importing its target module.

14. **Integration tests need Flask test client, not requests.** Use `app.test_client()` to test service endpoints without starting a real server. The Preparator SDK tests should mock the HTTP layer or use a test fixtures approach.

15. **Prometheus registry must be cleaned between test fixtures.** When re-importing service modules that call `create_service_counters()`, the Prometheus `CollectorRegistry` rejects duplicate metric names. Integration test fixtures must unregister all collectors before importing a new service's `app` package. See `_clear_app_modules()` in `test_service_endpoints.py`.

### Observability Lessons

16. **Centralized utilities eliminate boilerplate.** The `common/service_utils.py` module extracted duplicated code (Prometheus counters, /health, /metrics, X-Params parsing, metadata writing) from 11 services into shared functions. This ensures consistent behavior across all services.

17. **Correlation ID must be generated at the edge.** The Preparator SDK generates a UUID `correlation_id` on construction and includes it in every HTTP request (`X-Correlation-ID` header). Services read or generate it via `get_correlation_id()`. This enables end-to-end tracing of a pipeline request across all service logs.

18. **Structured logging must be opt-in at startup.** Using `configure_service_logging()` in `create_app()` (not at module level) prevents duplicate handlers when Flask reloads. The `JSONFormatter` outputs single-line JSON with timestamp, level, service, message, correlation_id, and dataset_name.

---

## 13. Technology Stack Reference

| Layer | Technology | Version | Notes |
|---|---|---|---|
| **Runtime** | Python | 3.9 | All services and tools |
| **Web framework** | Flask | 3.0.x | Blueprint pattern, app factory |
| **WSGI server** | Gunicorn | 21.2.x | 4 workers default |
| **Data format** | Apache Arrow IPC | PyArrow 18.x | Streaming format for all service data |
| **Data processing** | Pandas | 2.2.x | Arrow → Pandas → process → Arrow (consider pyarrow.compute for perf-critical paths) |
| **Orchestration** | Apache Airflow | latest | PostgreSQL backend, DAG-based |
| **AI (cloud)** | OpenAI API | GPT-4o-mini default | Pipeline generation |
| **AI (local)** | HuggingFace Transformers | Llama 3.2 1B Instruct | Text completion service |
| **UI** | Streamlit | 1.30+ | Chat + pipeline builder + data preview/download + health dashboard |
| **Containers** | Docker Compose | v2 | Single bridge network |
| **Monitoring** | Prometheus + Grafana | latest | Per-service metrics |
| **Testing** | pytest | 7.x+ | Unit + integration |
| **Linting** | ruff | 0.4+ | py39 target, 120 char lines |
| **CI/CD** | GitHub Actions | — | lint → test → build |

---

## 14. Common Tasks Cheat Sheet

| Task | Command / File |
|---|---|
| Start all services | `make up` or `docker compose up -d` |
| Run tests | `make test` |
| Lint | `make lint` |
| Add new service | Follow section 10 checklist |
| Generate benchmark data | `make benchmark-data` |
| Run benchmark | `make benchmark-all` |
| Trigger HR pipeline | Airflow UI → `hr_analytics_pipeline` → Trigger with config |
| Access Streamlit | http://localhost:8501 |

---

## 15. Remaining Technical Debt

Items that are known but not yet implemented:

1. **No OpenAPI/Swagger specs** — services lack formal API documentation. Consider `flasgger` for auto-generation.
2. **Arrow → Pandas → Arrow conversion** — most services still convert to Pandas for processing. For performance-critical transforms, use `pyarrow.compute` functions directly. Exception: `load_data` with `parquet` format already writes directly from Arrow.
3. **No auth on services** — services are open on the Docker network. For production, add API key or mTLS.
4. **No rate limiting** — services accept unlimited requests.
5. **Kubernetes manifests** — not implemented. Docker Compose only.
