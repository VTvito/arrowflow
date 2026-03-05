# Access & Credentials — ETL Microservices Platform

> **Security note:** Values shown here are **development defaults** defined in `.env.example`.  
> In production, override them all in the `.env` file (never commit `.env` to Git).

---

## Quick Reference — Service URLs

| Service | URL | Credentials |
|---|---|---|
| **Grafana** (monitoring dashboard) | http://localhost:3000 | `admin` / `change-me-strong-password` |
| **Prometheus** (raw metrics) | http://localhost:9090 | — no auth — |
| **Airflow** (DAG orchestration) | http://localhost:8080 | `admin` / `admin` (created on first boot) |
| **Streamlit UI** (AI pipeline builder) | http://localhost:8501 | — no auth — |
| **cAdvisor** (container metrics) | http://localhost:8088 | — no auth — |
| **PostgreSQL** (Airflow internal DB) | `localhost:5432` | `airflow` / `change-me-strong-password` |
| **statsd-exporter** (Airflow→Prometheus metrics) | http://localhost:9102/metrics | — no auth — |

---

## ETL Microservices — Ports & Health Endpoints

All services expose `GET /health` and `GET /metrics`.

| Container | Port | Health check |
|---|---|---|
| `extract-csv-service` | 5001 | http://localhost:5001/health |
| `clean-nan-service` | 5002 | http://localhost:5002/health |
| `delete-columns-service` | 5004 | http://localhost:5004/health |
| `extract-sql-service` | 5005 | http://localhost:5005/health |
| `extract-api-service` | 5006 | http://localhost:5006/health |
| `extract-excel-service` | 5007 | http://localhost:5007/health |
| `join-datasets-service` | 5008 | http://localhost:5008/health |
| `load-data-service` | 5009 | http://localhost:5009/health |
| `data-quality-service` | 5010 | http://localhost:5010/health |
| `outlier-detection-service` | 5011 | http://localhost:5011/health |
| `text-completion-llm-service` | 5012 | http://localhost:5012/health |

> **Port 5003** — unassigned (historical gap). Next available port: **5013**.

---

## Environment Variables — `.env`

Copy `.env.example` to `.env` and modify values before starting the stack.

```bash
cp .env.example .env
```

| Variable | Default | Description |
|---|---|---|
| `POSTGRES_USER` | `airflow` | PostgreSQL username |
| `POSTGRES_PASSWORD` | `change-me-strong-password` | PostgreSQL password ⚠️ change in production |
| `POSTGRES_DB` | `airflow` | PostgreSQL database name |
| `GF_SECURITY_ADMIN_PASSWORD` | `change-me-strong-password` | Grafana admin password ⚠️ change in production |
| `ETL_DATA_ROOT` | `/app/data` | Base directory for data in containers |
| `ALLOW_PRIVATE_API_URLS` | `false` | Allow private URLs in extract-api-service |
| `HF_MODELS_PATH` | `./hf_models` | Local HuggingFace model cache path |
| `LLM_PROVIDER` | `openai` | AI provider: `openai` or `local` |
| `OPENAI_API_KEY` | *(empty)* | OpenAI API key — required if `LLM_PROVIDER=openai` |
| `OPENAI_MODEL` | `gpt-4o-mini` | OpenAI model to use |
| `AIRFLOW_BASE_URL` | `http://localhost:8080` | Streamlit quick-trigger target Airflow URL (optional override) |
| `AIRFLOW_USERNAME` | `admin` | Streamlit quick-trigger Airflow username (optional override) |
| `AIRFLOW_PASSWORD` | `admin` | Streamlit quick-trigger Airflow password (optional override) |

> The Streamlit UI shows a warning when Airflow still uses default credentials.

---

## Airflow Credentials

The admin user is created automatically on first boot by the `CMD` in the Airflow Dockerfile
(idempotent: skipped if it already exists).

| Field | Default Value |
|---|---|
| Username | `admin` |
| Password | `admin` |
| URL | http://localhost:8080 |

> If you are using an instance that started **before** this change, create the user manually:
> ```bash
> docker exec airflow airflow users create \
>   --username admin --password admin \
>   --firstname Admin --lastname ETL \
>   --role Admin --email admin@etl.local
> ```
> Change the password after first login: `Admin → Users → admin → Edit`.

---

## Grafana — Login

| Field | Default Value |
|---|---|
| URL | http://localhost:3000 |
| Username | `admin` |
| Password | value of `GF_SECURITY_ADMIN_PASSWORD` in `.env` (default: `change-me-strong-password`) |

The **Prometheus** datasource and **ETL Microservices — Monitoring Overview** dashboard are pre-loaded automatically via provisioning (no manual configuration needed).

---

## PostgreSQL — Direct Connection

SQLAlchemy URI (used internally by Airflow, also useful for direct inspection):
```
postgresql+psycopg2://airflow:change-me-strong-password@localhost:5432/airflow
```

---

## Production Security Checklist

- [ ] Change `POSTGRES_PASSWORD` to a strong password
- [ ] Change `GF_SECURITY_ADMIN_PASSWORD` to a strong password  
- [ ] Set `OPENAI_API_KEY` to your real key
- [ ] Add authentication to microservices (currently open on internal network)
- [ ] Do not expose ETL ports (5001–5012) on public IPs
- [ ] Enable HTTPS for Grafana and Airflow in exposed environments
- [ ] Rotate Airflow admin credentials after first login
