# Extending the ETL Microservices Platform

This guide walks you through the two most common extension tasks:
1. **Adding a new microservice** (transform, extract, or load)
2. **Creating a new pipeline** (Airflow DAG or YAML for the AI agent)

---

## 1. Adding a New Microservice

### Overview

Every microservice in this platform follows the same structure:

```
services/my-service/
├── Dockerfile          # Container build (python:3.9-slim + gunicorn)
├── requirements.txt    # Python dependencies
├── run.py              # Local dev entry point (not used in Docker)
└── app/
    ├── __init__.py     # Flask app factory
    ├── routes.py       # HTTP endpoint + Prometheus metrics
    └── logic.py        # Pure business logic (no Flask imports)
```

The key design principle: **routes.py handles HTTP, logic.py handles data**. This separation makes logic unit-testable without HTTP scaffolding.

### Step-by-Step

#### 1.1 Copy the template

```bash
cp -r templates/new_service services/my-transform-service
```

#### 1.2 Replace placeholders

Open every file in `services/my-transform-service/` and replace:

| Placeholder | Example Value |
|---|---|
| `{{SERVICE_NAME}}` | `my-transform-service` |
| `{{SERVICE_SLUG}}` | `my_transform` |
| `{{SERVICE_PORT}}` | `5013` |
| `{{ENDPOINT_NAME}}` | `my-transform` |
| `{{LOGIC_MODULE}}` | `logic` |
| `{{LOGIC_FUNCTION}}` | `process_data` |

> **Port allocation:** Current max is 5012 (port 5003 is unused). Use **5013** for the next service.

#### 1.3 Implement business logic

Edit `app/logic.py`. Your function receives a `pyarrow.Table` and a `params` dict, and returns a `pyarrow.Table`:

```python
import pyarrow as pa
import pandas as pd

def my_transform(table: pa.Table, params: dict) -> pa.Table:
    df = table.to_pandas()

    # Your transformation here
    threshold = params.get('threshold', 0.5)
    df = df[df['score'] >= threshold]

    return pa.Table.from_pandas(df, preserve_index=False)
```

Update `routes.py` to import and call your function instead of `process_data`.

#### 1.4 Register the service

**preparator/services_config.json** — add the URL mapping:

```json
{
    "my_transform": "http://my-transform-service:5013/my-transform"
}
```

**schemas/service_registry.json** — add full metadata (used by the AI agent):

```json
{
    "name": "my_transform",
    "type": "transform",
    "description": "Filters rows by a score threshold",
    "endpoint": "/my-transform",
    "input_format": "arrow_ipc",
    "output_format": "arrow_ipc",
    "params": {
        "threshold": {
            "type": "number",
            "required": false,
            "default": 0.5,
            "description": "Minimum score to keep"
        }
    }
}
```

**docker-compose.yml** — add a service block:

```yaml
  my-transform-service:
    build:
      context: ./services
      dockerfile: my-transform-service/Dockerfile
    container_name: my-transform-service
    ports:
      - "5013:5013"
    volumes:
      - etl-containers-shared-data:/app/data
    networks:
      - etl-network
    restart: unless-stopped
```

**prometheus/prometheus.yml** — add a scrape target:

```yaml
  - job_name: 'my-transform-service'
    static_configs:
      - targets: ['my-transform-service:5013']
```

#### 1.5 Add a Preparator SDK method

Edit `preparator/preparator_v4.py`:

```python
def my_transform(self, ipc_data, dataset_name="default_dataset", threshold=0.5):
    """
    'my_transform' microservice.
    """
    params = {
        "dataset_name": dataset_name,
        "threshold": threshold,
    }
    return self.run_service_ipc_in_ipc_out("my_transform", ipc_data, params)
```

#### 1.6 Write tests

**tests/unit/test_logic.py:**

```python
import os
import sys

# Clear cached 'app' package to avoid namespace collision
for _mod in list(sys.modules):
    if _mod == "app" or _mod.startswith("app."):
        del sys.modules[_mod]

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "services", "my-transform-service"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "services"))

from app.logic import my_transform  # noqa: E402

def test_my_transform_filters_correctly(sample_arrow_table):
    result = my_transform(sample_arrow_table, {"threshold": 0.5})
    assert result.num_rows > 0
```

> **Important:** The `sys.modules` cleanup is mandatory. All services share the `app/` namespace, so Python must be told to forget previous `app.*` imports before loading a different service's module. See `tests/conftest.py` for shared fixtures.

#### 1.7 Build and test

```bash
# Build the new service
docker compose build my-transform-service

# Start it
docker compose up -d my-transform-service

# Check health
curl http://localhost:5013/health

# Run unit tests
python -m pytest tests/unit/test_logic.py -v
```

---

## 2. Creating a New Pipeline

### Option A: Airflow DAG

Create a new file in `airflow/dags/`. Follow the existing pattern:

```python
import json
import logging
from datetime import datetime

from airflow.decorators import task
from airflow.models.param import Param
from xcom_file_utils import cleanup_xcom_files, load_ipc_from_shared, save_ipc_to_shared

from airflow import DAG
from preparator.preparator_v4 import Preparator

logger = logging.getLogger("my_pipeline")
CONFIG_PATH = "/opt/airflow/preparator/services_config.json"

with DAG(
    "my_pipeline",
    default_args={"owner": "airflow", "start_date": datetime(2025, 1, 1), "retries": 1},
    schedule_interval=None,
    tags=["custom", "etl"],
    params={
        "dataset_name": Param("my_dataset", type="string"),
        "file_path": Param("/app/data/my_dataset/data.csv", type="string"),
        "output_format": Param("csv", type="string", enum=["csv", "xlsx", "json", "parquet"]),
        "use_file_xcom": Param(True, type="boolean"),
    },
) as dag:

    @task.python
    def extract(params: dict) -> str:
        with open(CONFIG_PATH) as f:
            config = json.load(f)
        with Preparator(config) as prep:
            ipc = prep.extract_csv(
                dataset_name=params["dataset_name"],
                file_path=params["file_path"],
            )
        if params.get("use_file_xcom", True):
            return save_ipc_to_shared(ipc, params["dataset_name"], "extract")
        return ipc.hex()

    @task.python
    def transform(data_ref: str, params: dict) -> str:
        with open(CONFIG_PATH) as f:
            config = json.load(f)
        use_file = params.get("use_file_xcom", True)
        ipc = load_ipc_from_shared(data_ref) if use_file else bytes.fromhex(data_ref)

        with Preparator(config) as prep:
            result = prep.clean_nan(ipc, dataset_name=params["dataset_name"])

        if use_file:
            return save_ipc_to_shared(result, params["dataset_name"], "transform")
        return result.hex()

    @task.python
    def load(data_ref: str, params: dict):
        with open(CONFIG_PATH) as f:
            config = json.load(f)
        use_file = params.get("use_file_xcom", True)
        ipc = load_ipc_from_shared(data_ref) if use_file else bytes.fromhex(data_ref)

        with Preparator(config) as prep:
            result = prep.load_data(ipc, format=params.get("output_format", "csv"),
                                    dataset_name=params["dataset_name"])

        if use_file:
            cleanup_xcom_files(params["dataset_name"])
        return result

    # DAG wiring
    extracted = extract()
    transformed = transform(extracted)
    load(transformed)
```

Key patterns:
- **File-based XCom** (`save_ipc_to_shared` / `load_ipc_from_shared`) for large datasets
- **Preparator as context manager** (`with Preparator(config) as prep:`)
- **Parameterized** via `Param()` for runtime configuration
- **Cleanup** XCom temp files in the final step

### Option B: YAML Pipeline (for Streamlit UI / AI Agent)

Create a YAML file following the pipeline schema (`schemas/pipeline_schema.json`):

```yaml
pipeline:
  name: my_pipeline
  description: My custom ETL pipeline
  steps:
    - id: extract
      service: extract_csv
      params:
        file_path: /app/data/my_dataset/data.csv

    - id: clean
      service: clean_nan
      params:
        strategy: fill_median
      depends_on: [extract]

    - id: save
      service: load_data
      params:
        format: csv
      depends_on: [clean]
```

You can paste this into the Streamlit YAML Editor tab, or save it as a file and use the AI agent to execute it.

Available services for `service` field:
- **Extract:** `extract_csv`, `extract_sql`, `extract_api`, `extract_excel`
- **Transform:** `clean_nan`, `delete_columns`, `join_datasets`, `data_quality`, `outlier_detection`, `text_completion_llm`
- **Load:** `load_data`

See `examples/pipelines/` for complete working examples.

---

## 3. Architecture Reference

### Communication Patterns

| Service Type | Request Body | Parameters | Response |
|---|---|---|---|
| Extract | JSON config | In JSON body | Arrow IPC binary |
| Transform | Arrow IPC binary | `X-Params` header (JSON string) | Arrow IPC binary |
| Load | Arrow IPC binary | `X-Params` header | JSON status |
| Join | Two Arrow IPC files (multipart form) | `X-Params` header | Arrow IPC binary |

### Shared Utilities (`services/common/`)

All services import these shared modules:

| Module | Key Functions |
|---|---|
| `arrow_utils.py` | `ipc_to_table()`, `table_to_ipc()` |
| `service_utils.py` | `create_service_counters()`, `register_standard_endpoints()`, `parse_x_params()`, `get_correlation_id()`, `save_metadata()` |
| `logging_config.py` | `configure_service_logging()`, `get_correlation_logger()` |
| `health.py` | `create_health_response()` |
| `path_utils.py` | `sanitize_dataset_name()`, `resolve_input_path()` |
| `json_utils.py` | `NpEncoder` (handles numpy types in JSON serialization) |

### Error Response Contract

All services return errors as:
```json
{"status": "error", "message": "Human-readable description"}
```
- `400` — Client error (bad parameters, missing data)
- `500` — Server error (processing failure)
