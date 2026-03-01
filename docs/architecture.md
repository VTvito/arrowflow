# Architecture & System Design — ETL Microservices Platform

Technical architecture document for contributors and developers. Explains _why_ the system is designed this way,
not just _how_ it works.

---

## Table of Contents

1. [Overview](#1-overview)
2. [Data Flow: Extract → Transform → Load](#2-data-flow-extract--transform--load)
3. [Inter-service Communication Protocol](#3-inter-service-communication-protocol)
4. [Internal Structure of Each Microservice](#4-internal-structure-of-each-microservice)
5. [Preparator SDK — Client-side Orchestration](#5-preparator-sdk--client-side-orchestration)
6. [Pipeline Compiler — Parallelism and Topological DAG](#6-pipeline-compiler--parallelism-and-topological-dag)
7. [AI Agent — Natural Language to Pipeline](#7-ai-agent--natural-language-to-pipeline)
8. [Multi-worker: Gunicorn and Implications](#8-multi-worker-gunicorn-and-implications)
9. [Observability: Tracing, Metrics, Logging](#9-observability-tracing-metrics-logging)
10. [Airflow and File-based XCom](#10-airflow-and-file-based-xcom)
11. [Security Design](#11-security-design)
12. [Trade-offs and Known Technical Debt](#12-trade-offs-and-known-technical-debt)

---

## 1. Overview

The platform implements a **decomposable ETL**: each data operation (extract, clean,
validate, load) is an independent Flask service. Orchestration is separated from execution.

```
                       ┌─────────────────────────────────────────────────────┐
                       │            Orchestration Layer                       │
                       │                                                      │
                       │  ┌──────────┐   ┌─────────────┐   ┌─────────────┐  │
                       │  │  Airflow │   │  AI Agent   │   │  Streamlit  │  │
                       │  │  (DAGs)  │   │ (NL → YAML) │   │    (UI)     │  │
                       │  └────┬─────┘   └──────┬──────┘   └──────┬──────┘  │
                       └───────┼────────────────┼─────────────────┼─────────┘
                               │                │                 │
                               └────────────────┴─────────────────┘
                                                │
                                    Preparator SDK (v4)
                                  HTTP + Arrow IPC + retry
                                                │
              ┌─────────────────────────────────┼──────────────────────────────────┐
              │                     ETL Services Layer                             │
              │                                                                    │
              │  Extract          Transform                          Load          │
              │  ────────         ─────────────────────────────     ────          │
              │  csv  :5001       clean-nan     :5002               :5009         │
              │  sql  :5005       delete-cols   :5004                             │
              │  api  :5006       data-quality  :5010                             │
              │  excel:5007       outliers      :5011                             │
              │                  join           :5008                             │
              │                  llm-complete   :5012                             │
              └────────────────────────────────────────────────────────────────────┘
                              │                       │
                    ┌─────────┴─────────┐   ┌─────────┴─────────┐
                    │  Shared Volume    │   │  Docker Network   │
                    │  /app/data        │   │  etl-network      │
                    │  (Arrow IPC,      │   │  (container DNS)  │
                    │   metadata,       │   └───────────────────┘
                    │   XCom files)     │
                    └───────────────────┘
```

**Guiding principle**: no service knows about the others. They communicate only via the Preparator.

---

## 2. Data Flow: Extract → Transform → Load

### Data format does not change between services

All data exchanges use **Apache Arrow IPC Streaming** (not CSV, not JSON).

```
[CSV File]
    │
    ▼
[extract-csv-service]
    │  POST /extract-csv   body: {"file_path": "...", "dataset_name": "..."}
    │  ← response: binary Apache Arrow IPC stream (~165 KB per 500 HR rows)
    │
    ▼ ipc_bytes
[clean-nan-service]
    │  POST /clean-nan   body: <Arrow IPC>   header X-Params: {"strategy": "fill_median"}
    │  ← response: transformed Arrow IPC
    │
    ▼ ipc_bytes
[load-data-service]
    │  POST /load-data   body: <Arrow IPC>   header X-Params: {"format": "parquet"}
    │  ← response: {"status": "ok", "path": "..."}  (JSON, only exception)
    ▼
[Parquet file on shared volume]
```

### Why Arrow IPC instead of CSV/JSON?

| Criterion | CSV/JSON | Arrow IPC |
|---|---|---|
| Payload size | 1x (baseline) | 0.3–0.6x (columnar + compression) |
| Parse overhead | High (row-by-row deserialization) | Nearly zero (zero-copy read) |
| Type safety | None (everything string) | Typed schema (int64, float64, date, ...) |
| Interoperability | Universal | Native in Python, Java, R, Rust, C++ |

HTTP+Arrow serialization overhead is ~5–15ms per hop, negligible compared to actual processing.

---

## 3. Inter-service Communication Protocol

There are **4 distinct patterns**, depending on service type:

### Pattern A — JSON in, Arrow IPC out (Extract services)

```
POST /extract-csv
Content-Type: application/json
X-Correlation-ID: <uuid>

{"file_path": "/app/data/demo/hr_sample.csv", "dataset_name": "hr"}

→ 200 OK
Content-Type: application/vnd.apache.arrow.stream
<binary Arrow IPC>
```

### Pattern B — Arrow IPC in, Arrow IPC out (Transform services)

```
POST /clean-nan
Content-Type: application/vnd.apache.arrow.stream
X-Params: {"strategy": "fill_median", "dataset_name": "hr"}
X-Correlation-ID: <uuid>

<binary Arrow IPC in body>

→ 200 OK
Content-Type: application/vnd.apache.arrow.stream
<binary transformed Arrow IPC>
```

Parameters go in the `X-Params` header (JSON string) because the body is already occupied by data.
This avoids URL length limits and supports complex nested objects.

### Pattern C — Multipart form (Join service)

The join requires two independent inputs → `multipart/form-data` with fields `file1` and `file2`
(both Arrow IPC) and parameters (`join_key`, `join_type`) in `X-Params`.

### Pattern D — Arrow IPC in, JSON out (Load service)

Only exception: `load-data-service` responds with JSON (`{"status": "ok", "path": "...", "rows": N}`).
Input body is always Arrow IPC; `X-Params` carries the output format (`csv`, `json`, `xlsx`, `parquet`).

---

## 4. Internal Structure of Each Microservice

Every service follows the Flask **app factory** pattern with Blueprint:

```
services/<name>/
├── Dockerfile              FROM python:3.9-slim, gunicorn 4 workers
├── requirements.txt
├── run.py                  dev entry point (not used in Docker)
└── app/
    ├── __init__.py         create_app(): factory, config, blueprint registration
    ├── routes.py           HTTP concerns: parse, dispatch, metrics, metadata
    └── <logic>.py          Pure data logic: no Flask imports
```

### Separation of concerns — routes.py vs logic.py

```
routes.py                          logic.py
─────────────────────────         ─────────────────────────────
parse request                     receive pa.Table / pd.DataFrame
read X-Params header              apply transformation
increment Prometheus counters      return pa.Table + stats dict
call logic function                (no Flask, no HTTP, no I/O)
deserialize Arrow IPC
call logic()
serialize result to Arrow IPC
write metadata JSON file
build Response
propagate X-Correlation-ID
```

Pure logic in `logic.py` is **testable without HTTP scaffolding** — just instantiate a
`pa.Table` and call the function. Unit tests never use `app.test_client()` for transformations.

### App factory with hard limits

`create_app()` configures three fundamental things: `MAX_CONTENT_LENGTH = 500 MB`, structured
JSON logging via `configure_service_logging()`, and JSON handlers for 404/413 (instead of
Flask's default HTML).

---

## 5. Preparator SDK — Client-side Orchestration

The Preparator (`preparator/preparator_v4.py`) is the **client layer** that abstracts all HTTP communication.
Airflow DAGs, AI Agent, Streamlit: all call exclusively via the Preparator.

### Design decisions

#### Connection pooling (requests.Session)

```python
self.session = requests.Session()
# A single Session reuses TCP connections between consecutive calls
# → eliminates TCP handshake overhead on each hop of the pipeline
```

#### Retry with exponential backoff

```python
retry_strategy = Retry(
    total=3,
    backoff_factor=0.5,       # sleep 0.5s, 1s, 2s between attempts
    status_forcelist=[502, 503, 504],  # only transient errors
    allowed_methods=["POST", "GET"],
)
```

Only **transient** errors (gateway/timeout) trigger retry. 4xx errors (bad request)
are not retried — they would be deterministic.

#### Asymmetric timeout

```python
DEFAULT_TIMEOUT = (5, 300)
# connect_timeout=5s: service must respond immediately to TCP connect
# read_timeout=300s:  but processing can take up to 5 minutes
# (e.g. LLM inference on large dataset)
```

#### Correlation ID propagated on every call

```python
self.correlation_id = correlation_id or str(uuid.uuid4())

# Every HTTP call includes:
headers={"X-Correlation-ID": self.correlation_id}
```

One `correlation_id` per Preparator instance → all hops of a single pipeline execution
share the same ID → logs from all services are linkable.

#### Context manager

```python
with Preparator(config) as prep:
    ipc = prep.extract_csv(dataset_name="hr", file_path="...")
    ipc = prep.clean_nan(ipc, strategy="fill_median")
    prep.load_data(ipc, format="parquet")
# session.close() called automatically in __exit__
```

---

## 6. Pipeline Compiler — Parallelism and Topological DAG

The `PipelineCompiler` (`ai_agent/pipeline_compiler.py`) executes pipeline YAML with
real parallelism when steps don't depend on each other.

### From YAML to graph

```yaml
steps:
  - id: extract          # no depends_on → layer 0
    service: extract_csv

  - id: quality          # depends on extract → layer 1
    depends_on: [extract]
    service: data_quality

  - id: clean            # depends on quality → layer 2
    depends_on: [quality]
    service: clean_nan

  - id: remove_cols      # depends on quality (not clean) → layer 2
    depends_on: [quality]
    service: delete_columns

  - id: save             # depends on both → layer 3
    depends_on: [clean, remove_cols]
    service: load_data
```

### Kahn's algorithm for topological layering

```
Layer 0:  [extract]                        → executed first
Layer 1:  [quality]                        → parallel (1 step)
Layer 2:  [clean, remove_cols]             → PARALLEL EXECUTION ✓
Layer 3:  [save]                           → executed last
```

The algorithm computes `in_degree` for each node; nodes with degree zero form the current layer,
then decrements the degrees of children and repeats. If the graph has a **cycle** (e.g. A depends on B which depends on A), Kahn terminates before visiting all nodes → `ValueError: cycle detected`. Validation happens before execution.

### Parallel execution with ThreadPoolExecutor

```python
def _execute_layer(self, layer, outputs, prep):
    """Execute all steps in the layer in parallel."""
    with ThreadPoolExecutor(max_workers=len(layer)) as executor:
        futures = {
            executor.submit(self._execute_step, step, outputs, prep): step
            for step in layer
        }
        for future in as_completed(futures):
            step = futures[future]
            result = future.result()           # propagates exceptions
            outputs[step["id"]] = result.data  # save output for downstream steps
```

**Thread safety**: each step writes to the `outputs` dict with a different key (its own `id`).
No shared data access → no race conditions.

**Note**: `ThreadPoolExecutor` is used (threads, not processes) because the bottleneck is I/O-bound
(HTTP calls), not CPU-bound. Python's GIL is not a problem here — threads release the GIL
during `socket.send()` and `socket.recv()`.

### Dispatch registry — zero if/elif

Instead of:
```python
# ❌ fragile, must update every time a service is added
if step["service"] == "extract_csv":
    result = prep.extract_csv(...)
elif step["service"] == "clean_nan":
    result = prep.clean_nan(...)
elif ...
```

The compiler uses a registry:
```python
# ✓ extensible: adding a service = adding 1 entry to the dict
registry = {
    "extract_csv":  lambda p, inp, ds, _: prep.extract_csv(file_path=p["file_path"], ...),
    "clean_nan":    lambda p, inp, ds, _: prep.clean_nan(inp, strategy=p.get("strategy","drop"), ...),
    "load_data":    lambda p, inp, ds, _: prep.load_data(inp, format=p.get("format","csv"), ...),
    # ...
}

handler = registry[step["service"]]
output  = handler(params, input_data, dataset_name, input_data_2)
```

---

## 7. AI Agent — NL to Pipeline

```
User: "extract hr_sample.csv, remove outliers, save as parquet"
         │
         ▼
  PipelineAgent.generate_pipeline()
         │
         ├─► Builds system prompt from service_registry.json
         │   (names, types, required and optional parameters of each service)
         │
         ├─► Calls LLM (OpenAI GPT-4o-mini or local HuggingFace)
         │
         ├─► Receives raw YAML
         │
         └─► validate_pipeline(yaml)
               │
               ├─ JSON Schema validation (pipeline_schema.json)
               ├─ Service names exist in registry
               ├─ Required parameters present
               ├─ depends_on references valid steps
               └─ No cycle in graph
                       │
                       ▼
            PipelineCompiler.execute(pipeline_def)
```

### LLM Provider abstraction

```python
class LLMProvider(ABC):
    @abstractmethod
    def complete(self, system_prompt, user_prompt) -> str: ...

class OpenAIProvider(LLMProvider):
    # GPT-4o-mini via API (requires OPENAI_API_KEY)

class LocalProvider(LLMProvider):
    # Calls text-completion-llm-service (HuggingFace Llama 3.2 1B Instruct)
    # Useful for air-gapped environments or testing without API costs
```

Selection via env var: `LLM_PROVIDER=openai` (default) or `LLM_PROVIDER=local`.

---

## 8. Multi-worker: Gunicorn and Implications

### Configuration

Every Flask service runs under **Gunicorn with 4 pre-fork workers**:

```
gunicorn --workers=4 --timeout=300 --bind=0.0.0.0:<port> app:create_app()
```

Exception: `text-completion-llm-service` uses **1 worker** (HuggingFace model ~1.5GB cannot
be replicated across 4 workers in memory).

### Gunicorn's pre-fork model

The master forks 4 independent worker processes (separate memory spaces). Nginx/any
reverse proxy does round-robin between them. The master handles OS signals and automatic
restart of crashed workers.

### Implication #1 — Distributed Prometheus counters

Prometheus counters (`Counter(...)`) are in-process memory. Each worker maintains its
**own** counters. When Prometheus scrapes `GET /metrics`, it reaches only one worker per request.

**Practical effect**: the `extract_csv_requests_total` counter you see in Prometheus is the value
from the single worker that responded to the scrape request — typically ~1/4 of actual requests
(with 4 workers and round-robin distribution).

**Unimplemented workaround** (known technical debt): `prometheus_client` supports
[multiprocess mode](https://github.com/prometheus/client_python#multiprocess-mode-eg-gunicorn)
to aggregate counters from all workers via a shared directory:

```python
# Would require:
os.environ["PROMETHEUS_MULTIPROC_DIR"] = "/tmp/prometheus_multiproc"
from prometheus_client import CollectorRegistry, multiprocess
```

**Practical implication for monitoring**: _trends_ and _rates_ remain significant and
reliable for detecting anomalies. Absolute values are underestimated.

### Implication #2 — Lazy loading for LLM model

```python
# text-completion-llm-service/app/completion.py
_model = None
_tokenizer = None

def get_model():
    global _model, _tokenizer
    if _model is None:
        _model = AutoModelForCausalLM.from_pretrained(MODEL_PATH)
        _tokenizer = AutoTokenizer.from_pretrained(MODEL_PATH)
    return _model, _tokenizer
```

The model is loaded on **first request**, not at container startup. This allows the
container to become "healthy" in ~3s instead of 30+s. First request will have higher
latency (~15–30s cold start).

### Implication #3 — Gunicorn timeout and long pipelines

```
gunicorn --timeout=300  # 5 minutes per request
```

The longest estimated pipeline is LLM inference on 500k rows (~4 min). Preparator SDK uses
`read_timeout=300s` by default. If pipeline exceeds 5 minutes, Gunicorn kills the worker
and client receives `502 Bad Gateway` → retry strategy triggers backoff.

---

## 9. Observability: Tracing, Metrics, Logging

### Correlation ID — distributed tracing without Jaeger

```
Preparator.__init__()
    │  generates UUID → self.correlation_id
    │
    ├─► POST /extract-csv    X-Correlation-ID: abc-123
    ├─► POST /clean-nan      X-Correlation-ID: abc-123
    ├─► POST /load-data      X-Correlation-ID: abc-123
```

Every service reads the correlation ID from the header (or generates a new one if absent) via
`get_correlation_id()` from `common/service_utils.py`, includes it in structured logs, and
propagates it in the response. Search for correlation ID in logs to **reconstruct the entire trace**.

### Structured logging JSON

Every log line is JSON on a single line with fields: `timestamp`, `level`, `service`, `message`,
`correlation_id`, `dataset_name`. Directly filterable with `jq`:

```bash
docker compose logs | jq 'select(.correlation_id == "abc-123")'
```

### Metadata files for audit trail

Every service writes a JSON file to `/app/data/<dataset_name>/metadata/` after processing:

```json
{
  "service": "clean-nan-service",
  "dataset_name": "hr",
  "timestamp": "2026-03-01T18:40:01Z",
  "rows_in": 501,
  "rows_out": 498,
  "duration_sec": 0.043,
  "strategy": "fill_median",
  "columns_affected": 3,
  "nulls_filled": 12
}
```

These files are low-latency (written locally on shared volume) and allow
reconstructing what happened at each step without depending on external systems.

### Prometheus scraping

```
every 15 seconds
    Prometheus → GET /metrics on each service → saves counters to TSDB
    retention: 15 days
    storage: /etc/prometheus/data (volume etl-prometheus-data)
```

---

## 10. Airflow and File-based XCom

### The problem of PostgreSQL XCom with binary data

Airflow by default uses PostgreSQL to pass data between tasks via XCom (cross-communication).
The practical limit of PostgreSQL for bytea is ~1MB; for Arrow IPC datasets with 50k rows
this limit is easily exceeded.

### Solution: file-based XCom

```python
# airflow/dags/xcom_file_utils.py

def save_ipc_to_shared(ipc_data, dataset_name, step_name) -> str:
    """Saves Arrow IPC to /app/data/<ds>/xcom/<step>_<ts>_<uuid>.arrow"""
    path = f"/app/data/{dataset_name}/xcom/{step_name}_{ts}_{uuid}.arrow"
    with open(path, "wb") as f:
        f.write(ipc_data)
    return path  # ← returns only the path (string, <100 bytes) via XCom Postgres

def load_ipc_from_shared(file_path) -> bytes:
    with open(file_path, "rb") as f:
        return f.read()
```

The `etl-containers-shared-data` volume is mounted in all containers (Airflow, all
microservices) at `/app/data` → files written by one task are readable by the next task.

```
 XCom Postgres:  task_A → task_B   =  "/app/data/hr/xcom/extract_20260301T184000Z_abc.arrow"
                                                   ↑
                                        small string → ok for Postgres
 File system:    task_A → task_B   = Arrow IPC binary (165 KB, 2 MB, 50 MB...)
                                                   ↑
                                        actual data → ok for shared filesystem
```

---

## 11. Security Design

### Path traversal protection

```python
# services/common/path_utils.py
def resolve_input_path(file_path: str) -> str:
    allowed_root = os.environ.get("ETL_DATA_ROOT", "/app/data")
    resolved = os.path.realpath(os.path.join(allowed_root, file_path.lstrip("/")))

    if not resolved.startswith(os.path.realpath(allowed_root)):
        raise ValueError(f"Parameter 'file_path' must stay under {allowed_root}")

    return resolved
```

No service accepts arbitrary paths. `os.path.realpath` resolves symlinks and `..` before
checking. Attempting `/etc/passwd` or `../../secrets` returns HTTP 400.

### API URL validation (extract-api-service)

If `ALLOW_PRIVATE_API_URLS=false` (default), the service blocks URLs to:
- `localhost`, `127.*`, `10.*`, `192.168.*`, `172.16–31.*` (RFC 1918)
- Cloud metadata: `169.254.169.254` (AWS/GCP/Azure IMDS)

Prevents SSRF (Server-Side Request Forgery) in Docker environments where other containers are
reachable via internal DNS.

### SQL injection protection

```python
# extract-sql service does not accept multiple statements
if ";" in query.rstrip(";"):
    raise ValueError("Multiple SQL statements are not allowed")

# Whitelist of allowed prefixes
ALLOWED_PREFIXES = ("SELECT ", "WITH ")
if not query.strip().upper().startswith(ALLOWED_PREFIXES):
    raise ValueError("Only SELECT/WITH queries are allowed")
```

---

## 12. Trade-offs and Known Technical Debt

### Deliberate Decisions

| Decision | Alternative Rejected | Rationale |
|---|---|---|
| Arrow IPC over HTTP | gRPC + Protobuf | simpler setup, no proto schema needed |
| Thread pool for parallelism | asyncio | more readable, I/O-bound doesn't benefit from async |
| File-based XCom | Redis/Celery | no extra dependencies, uses already-present volume |
| X-Params header | URL query params | supports nested objects, no URL length limit |
| `python:3.9-slim` | newer images | stability; Arrow 18.x supports 3.9 |

### Known Technical Debt

1. **Prometheus multi-process mode not enabled** — Gunicorn counters are per-worker.
   Trends are reliable, absolute values underestimated by ~1/4x.

2. **Arrow → Pandas → Arrow** — most transforms convert to Pandas for processing. For datasets >1M rows
   use `pyarrow.compute` directly. Exception: `load-data-service` with Parquet writes directly
   from Arrow (via `pyarrow.parquet`).

3. **No authentication on microservices** — open on Docker internal network.
   For production: API key header or mTLS between services.

4. **Prometheus counters reset on container restart** — `restart: always` in docker-compose
   is a workaround for crashes, but zeroes metrics. Consider Pushgateway for persistent metrics.

5. **No Kubernetes manifests** — Docker Compose only. For horizontal scaling (multiple workers
   of the same service behind a load balancer) requires migration to K8s + Horizontal Pod
   Autoscaler.
