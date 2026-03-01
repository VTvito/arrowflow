# Access & Credentials — ETL Microservices Platform

> **Security note:** I valori mostrati qui sono i **default di sviluppo** definiti in `.env.example`.  
> In produzione, sovrascrivili tutti nel file `.env` (mai committare `.env` su Git).

---

## Quick Reference — Service URLs

| Servizio | URL | Credenziali |
|---|---|---|
| **Grafana** (dashboard monitoraggio) | http://localhost:3000 | `admin` / `change-me-strong-password` |
| **Prometheus** (metriche raw) | http://localhost:9090 | — nessuna auth — |
| **Airflow** (orchestrazione DAG) | http://localhost:8080 | `admin` / `admin` (creato al primo avvio) |
| **Streamlit UI** (AI pipeline builder) | http://localhost:8501 | — nessuna auth — |
| **cAdvisor** (container metrics) | http://localhost:8088 | — nessuna auth — |
| **PostgreSQL** (DB interno Airflow) | `localhost:5432` | `airflow` / `change-me-strong-password` |
| **statsd-exporter** (metrics Airflow→Prom) | http://localhost:9102/metrics | — nessuna auth — |

---

## ETL Microservices — Porte & Health Endpoints

Tutti i servizi espongono `GET /health` e `GET /metrics`.

| Container | Porta | Health check |
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

> **Porta 5003** — non assegnata (gap storico). Prossimo servizio disponibile: **5013**.

---

## Variabili d'Ambiente — `.env`

Copia `.env.example` in `.env` e modifica i valori prima di avviare lo stack.

```bash
cp .env.example .env
```

| Variabile | Default | Descrizione |
|---|---|---|
| `POSTGRES_USER` | `airflow` | Username PostgreSQL |
| `POSTGRES_PASSWORD` | `change-me-strong-password` | Password PostgreSQL ⚠️ cambia in produzione |
| `POSTGRES_DB` | `airflow` | Nome database PostgreSQL |
| `GF_SECURITY_ADMIN_PASSWORD` | `change-me-strong-password` | Password admin Grafana ⚠️ cambia in produzione |
| `ETL_DATA_ROOT` | `/app/data` | Root directory dati nei container |
| `ALLOW_PRIVATE_API_URLS` | `false` | Permette URL privati nell'extract-api-service |
| `HF_MODELS_PATH` | `./hf_models` | Path locale cache modelli HuggingFace |
| `LLM_PROVIDER` | `openai` | Provider AI: `openai` oppure `local` |
| `OPENAI_API_KEY` | *(vuoto)* | API key OpenAI — richiesta se `LLM_PROVIDER=openai` |
| `OPENAI_MODEL` | `gpt-4o-mini` | Modello OpenAI da usare |

---

## Credenziali Airflow

L'utente admin viene creato automaticamente al primo avvio dal `CMD` nel Dockerfile Airflow
(idempotente: se esiste già non lo ricrea).

| Campo | Valore default |
|---|---|
| Username | `admin` |
| Password | `admin` |
| URL | http://localhost:8080 |

> Se stai usando un'istanza già avviata **prima** di questa modifica, crea l'utente manualmente:
> ```bash
> docker exec airflow airflow users create \
>   --username admin --password admin \
>   --firstname Admin --lastname ETL \
>   --role Admin --email admin@etl.local
> ```
> Cambia la password dopo il primo login: `Admin → Users → admin → Edit`.

---

## Grafana — Login

| Campo | Valore default |
|---|---|
| URL | http://localhost:3000 |
| Username | `admin` |
| Password | valore di `GF_SECURITY_ADMIN_PASSWORD` nel `.env` (default: `change-me-strong-password`) |

Il datasource **Prometheus** e il dashboard **ETL Microservices — Monitoring Overview** sono pre-caricati automaticamente tramite provisioning (non serve configurazione manuale).

---

## PostgreSQL — Connessione diretta

SQLAlchemy URI (usata internamente da Airflow, utile anche per ispezione diretta):
```
postgresql+psycopg2://airflow:change-me-strong-password@localhost:5432/airflow
```

---

## Checklist Sicurezza (Produzione)

- [ ] Cambia `POSTGRES_PASSWORD` con una password forte
- [ ] Cambia `GF_SECURITY_ADMIN_PASSWORD` con una password forte  
- [ ] Imposta `OPENAI_API_KEY` con la tua chiave reale
- [ ] Aggiungi autenticazione ai microservizi (attualmente open su rete interna)
- [ ] Non esporre le porte ETL (5001–5012) su IP pubblici
- [ ] Abilita HTTPS per Grafana e Airflow in ambienti esposti
- [ ] Ruota credenziali Airflow admin dopo il primo login
