# Copilot Instructions — ETL Microservices Platform

## Project Overview

This is a **modular ETL (Extract, Transform, Load) platform** built with a microservices architecture. Each ETL operation (extract from CSV, clean NaN, detect outliers, etc.) is an independent Flask-based microservice, orchestrated via Apache Airflow DAGs. The platform uses **Apache Arrow IPC** as the wire format for high-performance binary data interchange between services.

### Core Value Proposition

Dynamic, composable ETL pipelines where each step is an independently deployable, observable, and scalable microservice. The **Preparator** class provides a Pythonic SDK that abstracts HTTP communication, enabling pipeline composition in code.

---

## Technology Stack

| Layer | Technology | Purpose |
|---|---|---|
| **Microservices** | Python 3.9, Flask | Stateless ETL operation services |
| **Data Format** | Apache Arrow IPC (streaming) | Zero-copy columnar binary data interchange |
| **Orchestration** | Apache Airflow | DAG-based pipeline scheduling and monitoring |
| **Containerization** | Docker, Docker Compose | Service isolation and deployment |
| **Monitoring** | Prometheus + Grafana | Metrics collection and visualization |
| **Metrics Export** | StatsD Exporter, prometheus_client | Bridge Airflow/service metrics to Prometheus |
| **Database** | PostgreSQL | Airflow metadata backend |
| **AI/ML** | HuggingFace Transformers, PyTorch | LLM-based text completion service |
| **Data Processing** | Pandas, PyArrow, NumPy | In-service data manipulation |

---

## Architecture

### High-Level Data Flow

```
[Data Source] → [Extract Service] → Arrow IPC → [Transform Service(s)] → Arrow IPC → [Load Service] → [Output]
                                         ↑                                      ↑
                                    Airflow DAG orchestrates via Preparator SDK
```

### Service Communication Patterns

There are **two communication patterns** used by microservices:

1. **JSON-in / Arrow IPC-out** — Used by **Extract** services:
   - Client sends a JSON body with parameters (file path, DB URL, API endpoint, etc.)
   - Service returns Arrow IPC binary data in the response body
   - Content-Type response: `application/vnd.apache.arrow.stream`

2. **Arrow IPC-in / Arrow IPC-out** — Used by **Transform** and **Load** services:
   - Client sends Arrow IPC binary data in the request body
   - Parameters are passed via the `X-Params` HTTP header (JSON-encoded string)
   - Content-Type request: `application/vnd.apache.arrow.stream`
   - Service returns transformed Arrow IPC binary data (or JSON for load-data)

3. **Multipart form-data** — Used by **join-datasets-service**:
   - Two Arrow IPC files sent as `dataset1` and `dataset2` form fields
   - Parameters in `X-Params` header

### The Preparator SDK

The `Preparator` class (in `preparator/`) is the **client-side orchestration layer**. It provides typed Python methods that wrap HTTP calls to microservices. There are two versions:

- **`preparator_v3.py`** — Passes parameters via URL query strings (`urllib.parse.urlencode`)
- **`preparator_v4.py`** — Passes parameters via `X-Params` HTTP header (JSON). This is the **current/preferred version**.

The Preparator uses `requests.Session()` for connection pooling. Key method categories:
- `extract_*()` → calls extract services (JSON in, IPC out)
- `clean_nan()`, `delete_columns()`, `detect_outliers()`, `check_quality()` → transform services (IPC in, IPC out)
- `join_datasets()` → multipart request with two datasets
- `text_completion_llm()` → LLM-based text imputation
- `load_data()` → final output/save service

### Service Registry

Service endpoints are defined in `preparator/services_config.json`. This file maps logical service keys to HTTP URLs. All services run on the `etl-network` Docker bridge network and communicate by container name.

```json
{
  "extract_csv": "http://extract-csv-service:5001/extract-csv",
  "clean_nan": "http://clean-nan-service:5002/clean-nan",
  ...
}
```

---

## Microservices Catalog

### Extract Services (JSON-in → Arrow IPC-out)

| Service | Port | Key | Description |
|---|---|---|---|
| `extract-csv-service` | 5001 | `extract_csv` | Reads CSV files from shared volume, returns Arrow IPC |
| `extract-sql-service` | 5005 | `extract_sql` | Executes SQL query via SQLAlchemy, returns Arrow IPC |
| `extract-api-service` | 5006 | `extract_api` | Fetches data from REST APIs (supports API key auth) |
| `extract-excel-service` | 5007 | `extract_excel` | Reads .xls/.xlsx files, returns Arrow IPC |

### Transform Services (Arrow IPC-in → Arrow IPC-out)

| Service | Port | Key | Description |
|---|---|---|---|
| `clean-nan-service` | 5002 | `clean_nan` | Drops rows with null values |
| `delete-columns-service` | 5004 | `delete_columns` | Removes specified columns |
| `join-datasets-service` | 5008 | `join_datasets` | Joins two datasets (inner/left/right/outer) |
| `data-quality-service` | 5010 | `data_quality` | Runs quality checks (min rows, null ratio), pass-through |
| `outlier-detection-service` | 5011 | `outlier_detection` | Z-score outlier detection and removal on a column |
| `text-completion-llm-service` | 5012 | `text_completion_llm` | LLM text generation to fill placeholders in text columns |

### Load Services

| Service | Port | Key | Description |
|---|---|---|---|
| `load-data-service` | 5009 | `load_data` | Converts Arrow IPC to CSV/Excel/JSON and saves to disk |

---

## Project Structure

```
├── docker-compose.yml              # Full stack definition (all services + infra)
├── airflow/
│   ├── Dockerfile                  # Airflow image (webserver + scheduler)
│   └── dags/                       # Airflow DAG definitions
│       ├── parametrized_preparator_v4_quality.py    # Main pipeline (extract → quality → outliers → clean → load)
│       ├── parametrized_preparator_v4_ia.py         # Pipeline with LLM text completion
│       └── parametrized_preparator_v4_quality_join.py # Pipeline with dataset join
├── preparator/
│   ├── services_config.json        # Service registry (key → URL mapping)
│   ├── preparator_v3.py            # SDK v3 (query params) — legacy
│   └── preparator_v4.py            # SDK v4 (X-Params header) — current
├── services/
│   ├── common/                     # Shared utilities across all services
│   │   ├── arrow_utils.py          # Arrow IPC serialization/deserialization (ipc_to_table, table_to_ipc)
│   │   └── json_utils.py           # NumPy-safe JSON encoder (NpEncoder)
│   ├── <service-name>/
│   │   ├── Dockerfile              # Service container definition
│   │   ├── requirements.txt        # Python dependencies
│   │   ├── run.py                  # Flask app entry point
│   │   └── app/
│   │       ├── __init__.py         # Flask app factory (create_app)
│   │       ├── routes.py           # HTTP endpoints + Prometheus metrics
│   │       └── <logic>.py          # Pure business logic (no HTTP concerns)
│   └── ...
└── prometheus/
    └── prometheus.yml              # Prometheus scrape targets
```

---

## Coding Conventions and Patterns

### Service Structure Pattern

Every microservice follows this exact structure:

1. **`run.py`** — Entry point, creates Flask app and runs on assigned port
2. **`app/__init__.py`** — App factory pattern: `create_app()` registers blueprints
3. **`app/routes.py`** — Blueprint with HTTP endpoints + Prometheus counters (REQUEST/SUCCESS/ERROR)
4. **`app/<logic>.py`** — Pure data transformation function(s), no Flask/HTTP dependencies

### Endpoint Pattern (Transform Services)

Every transform service endpoint follows this exact flow:

```python
@bp.route('/<service-name>', methods=['POST'])
def handler():
    start_time = time.time()
    try:
        REQUEST_COUNTER.inc()
        # 1. Parse X-Params header (JSON)
        params = json.loads(request.headers.get('X-Params', '{}'))
        dataset_name = params.get('dataset_name')
        # 2. Validate required parameters
        # 3. Read Arrow IPC from request body
        ipc_data = request.get_data()
        table_in = ipc_to_table(ipc_data)
        # 4. Apply transformation (call business logic)
        result_table = apply_transformation(table_in, ...)
        # 5. Serialize result back to Arrow IPC
        out_ipc = table_to_ipc(result_table)
        # 6. Save metadata JSON to /app/data/<dataset_name>/metadata/
        SUCCESS_COUNTER.inc()
        return Response(out_ipc, mimetype="application/vnd.apache.arrow.stream"), 200
    except Exception:
        ERROR_COUNTER.inc()
        return jsonify({"status": "error", "message": str(e)}), 500
```

### Metadata Pattern

Every service writes a JSON metadata file after processing:
- Path: `/app/data/<dataset_name>/metadata/metadata_<service>_<timestamp>.json`
- Timestamp format: `%Y%m%dT%H%M%SZ` (UTC)
- Contains: service name, dataset name, row/column counts, duration, service-specific fields

### Arrow Utilities (common/)

Shared across all services:
- `ipc_to_table(bytes) → pa.Table` — Deserialize Arrow IPC stream to table
- `table_to_ipc(pa.Table) → bytes` — Serialize Arrow table to IPC stream
- `NpEncoder` — JSON encoder that handles NumPy types (int64, float64, ndarray, bool_)

### Prometheus Metrics

Every service exposes:
- `GET /metrics` — Prometheus scrape endpoint
- Three counters per service: `<name>_requests_total`, `<name>_success_total`, `<name>_error_total`

---

## Adding a New Microservice

When creating a new microservice, follow these steps:

1. **Create service directory**: `services/<service-name>/`
2. **Implement the logic module**: `app/<logic>.py` — Pure function, takes Arrow Table + params, returns Arrow Table
3. **Implement routes**: `app/routes.py` — Follow the endpoint pattern above
4. **Create `app/__init__.py`**: Use the app factory with blueprint registration
5. **Create `run.py`**: Entry point with assigned port number
6. **Create `Dockerfile`**: Follow existing pattern — copy `common/`, install deps, set PYTHONPATH
7. **Create `requirements.txt`**: Always include Flask, pyarrow, prometheus_client, pandas, numpy
8. **Register in `services_config.json`**: Add the new service key → URL mapping
9. **Add to `docker-compose.yml`**: New service block with shared volume and network
10. **Add to `prometheus/prometheus.yml`**: New scrape target
11. **Add method to `preparator_v4.py`**: Wrap the HTTP call in a typed Python method
12. **Port assignment**: Use the next available port (current max: 5012)

---

## Docker & Infrastructure

### Volumes

| Volume | Purpose |
|---|---|
| `etl-containers-shared-data` | Shared data between all services (datasets, metadata, outputs) mounted at `/app/data` |
| `etl-data-airflow` | Airflow persistence (`/opt/airflow`) |
| `etl-postgres-data` | PostgreSQL data directory |
| `etl-grafana-data` | Grafana dashboards and configuration |
| `etl-prometheus-data` | Prometheus TSDB data |

### Network

All services communicate on `etl-network` (Docker bridge). Services reference each other by container name (e.g., `http://clean-nan-service:5002`).

### Airflow Configuration

- Airflow connects to PostgreSQL via `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN`
- DAGs are mounted from `./airflow/dags` to `/opt/airflow/dags`
- Preparator is mounted from `./preparator` to `/opt/airflow/preparator`
- PYTHONPATH includes `/opt/airflow/` so DAGs can import `preparator.preparator_v4`

---

## Important Implementation Notes

### Data Flow Invariant
All data between services flows as **Apache Arrow IPC streaming format**. Never use CSV/JSON for inter-service data transfer. The only exception is the extract services (which accept JSON config) and load-data-service (which returns JSON status).

### Parameter Passing Convention (v4)
Transform/Load services receive parameters in the `X-Params` HTTP header as a JSON string. Extract services receive parameters in the JSON request body. Always use `preparator_v4.py` patterns for new code.

### Business Logic Isolation
Keep HTTP/Flask code in `routes.py`. Keep data transformation logic in the dedicated module (e.g., `clean.py`, `dq.py`). The logic module should work with `pyarrow.Table` or `pandas.DataFrame` objects only — no Flask request/response objects.

### Error Handling
All services return:
- `200` + Arrow IPC body on success (or JSON for load-data)
- `400` + JSON `{"status": "error", "message": "..."}` for client errors
- `500` + JSON `{"status": "error", "message": "..."}` for server errors

### Current Data Processing Approach
Services currently convert Arrow → Pandas → process → Arrow. For performance-critical paths, prefer using `pyarrow.compute` functions directly to avoid the conversion overhead.

---

## Known Issues and Technical Debt

1. **Flask dev server in production** — All services run with `app.run()` (single-threaded). Should use gunicorn/uvicorn.
2. **No health check endpoints** — Services lack `GET /health` endpoints for container orchestration.
3. **No tests** — No unit or integration tests exist.
4. **No CI/CD pipeline** — No automated build/test/deploy.
5. **No API documentation** — No OpenAPI/Swagger specs.
6. **Hardcoded paths** — File paths like `/app/data/` are hardcoded; should use environment variables.
7. **No request tracing** — No correlation ID to track a pipeline execution across services.
8. **XCom limitations** — Airflow XCom is not suitable for large binary data transfer (>50k rows).
9. **Kubernetes manifests** — Referenced in README but not implemented.
10. **Duplicate DAG files** — Several "copy" variants should be consolidated.
11. **Bug in load-data-service** — Missing comma in format validation list: `'xls' 'json'` should be `'xls', 'json'`.

---

## Future Direction: AI-Driven Pipeline Orchestration

The long-term vision is to evolve this platform into an **AI-driven self-service ETL system** where:

1. A **Pipeline Definition Schema** (YAML/JSON) declaratively describes ETL workflows
2. A **Service Registry** dynamically exposes available ETL capabilities and their parameters
3. An **AI Agent/Chatbot** translates natural language requests into pipeline definitions
4. Pipelines are automatically validated, compiled into Airflow DAGs, and executed
5. The agent can also **generate new microservice skeletons** based on user requirements

This transforms the platform from a pure microservices ETL architecture into a **composable, AI-orchestrated data engineering platform**.
