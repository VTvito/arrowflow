# ETL Microservices Platform

A modular ETL (Extract, Transform, Load) platform built with a microservices architecture. Each ETL operation is an independent Flask-based microservice, orchestrated via Apache Airflow DAGs. The platform uses **Apache Arrow IPC** as the wire format for high-performance binary data interchange between services.

---

## 1. Architecture

Data flows through the pipeline as Apache Arrow IPC binary streams:

```
[Source] → [Extract Service] → Arrow IPC → [Transform Service(s)] → Arrow IPC → [Load Service] → [Output]
                                      ↑
                           Airflow DAG orchestrates via Preparator SDK
```

### ETL Microservices

| Service | Port | Description |
|---|---|---|
| `extract-csv-service` | 5001 | Reads CSV files, returns Arrow IPC |
| `clean-nan-service` | 5002 | Drops rows with null values |
| `delete-columns-service` | 5004 | Removes specified columns |
| `extract-sql-service` | 5005 | Executes SQL queries via SQLAlchemy |
| `extract-api-service` | 5006 | Fetches data from REST APIs |
| `extract-excel-service` | 5007 | Reads Excel (.xls/.xlsx) files |
| `join-datasets-service` | 5008 | Joins two datasets (inner/left/right/outer) |
| `load-data-service` | 5009 | Exports data to CSV/Excel/JSON |
| `data-quality-service` | 5010 | Validates quality rules (null ratio, min rows) |
| `outlier-detection-service` | 5011 | Z-score outlier detection and removal |
| `text-completion-llm-service` | 5012 | LLM-based text placeholder completion |

Each service exposes:
- `POST /<service-name>` — main ETL endpoint
- `GET /health` — health check for container orchestration
- `GET /metrics` — Prometheus scrape endpoint

Services are structured modularly, separating HTTP routing (`routes.py`) from data transformation logic. The **Preparator** SDK (`preparator/preparator_v4.py`) provides a typed Python client that abstracts all HTTP communication, enabling pipeline composition in code.

---

### Airflow

- Orchestrates ETL pipelines via **DAGs** (Directed Acyclic Graphs)
- Uses **PostgreSQL** for metadata persistence
- DAGs use the `Preparator` SDK to call microservices

Available DAGs:

| DAG | Description |
|---|---|
| `parametrized_preparator_v4_quality` | Extract → Quality Check → Outlier Detection → Clean NaN → Load |
| `parametrized_preparator_v4_ia` | Pipeline with LLM text completion step |
| `parametrized_preparator_v4_quality_join` | Pipeline with dataset join |
| `parametrized_preparator_v4_ia_multitasks_v1` | Multi-task pipeline with XCom base64 encoding |
| `parametrized_preparator_v4_ia_multitasks_v2` | Streamlined multi-task pipeline |

---

### Monitoring

- **Prometheus** (port 9090): Collects metrics from all microservices and Airflow
- **Grafana** (port 3000): Visualizes metrics and dashboards
- **StatsD Exporter**: Bridges Airflow metrics to Prometheus

---

### Infrastructure

- **PostgreSQL**: Airflow metadata database
- **Docker Compose**: Full-stack local deployment
- Shared volume `etl-containers-shared-data` mounted at `/app/data` across all services

---

## 2. Installation Guide

### Requirements

- **Docker Desktop** (with Docker Compose)

---

### Running with Docker Compose

1. **Clone the repository**:
   ```bash
   git clone https://github.com/VTvito/etl_microservices.git
   cd etl_microservices
   ```

2. **Build and start all services**:
   ```bash
   docker-compose build
   docker-compose up -d
   ```

3. **Initialize the Airflow database and create an admin user**:
   ```bash
   docker exec -it airflow airflow db init
   docker exec -it airflow airflow users create \
     --username admin --firstname Admin --lastname User \
     --role Admin --email admin@example.com --password admin
   ```

4. **Copy datasets into the shared volume**:
   ```bash
   docker cp /path/to/dataset.csv extract-csv-service:/app/data/
   ```

5. **Access the web interfaces**:

   | Interface | URL |
   |---|---|
   | Airflow | http://localhost:8080 |
   | Prometheus | http://localhost:9090 |
   | Grafana | http://localhost:3000 |

Processed output files are stored in the Docker volume `etl-containers-shared-data` under `/app/data/<dataset_name>/processed/`.

---

## 3. Service Communication

- **Extract services**: Accept JSON body, return Arrow IPC
- **Transform services**: Accept Arrow IPC body + `X-Params` JSON header, return Arrow IPC
- **Load service**: Accepts Arrow IPC body + `X-Params` JSON header, saves file and returns JSON status

The `Preparator` class handles all HTTP communication automatically when composing pipelines.
