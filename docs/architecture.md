# Architecture & System Design — ETL Microservices Platform

Documento tecnico per contributor e sviluppatori. Spiega _perché_ il sistema è fatto così,
non solo _come_ è fatto.

---

## Indice

1. [Visione d'insieme](#1-visione-dinsieme)
2. [Flusso dati: Extract → Transform → Load](#2-flusso-dati-extract--transform--load)
3. [Protocollo di comunicazione inter-service](#3-protocollo-di-comunicazione-inter-service)
4. [Struttura interna di ogni microservizio](#4-struttura-interna-di-ogni-microservizio)
5. [Preparator SDK — client-side orchestration](#5-preparator-sdk--client-side-orchestration)
6. [Pipeline Compiler — parallelismo e DAG topologico](#6-pipeline-compiler--parallelismo-e-dag-topologico)
7. [AI Agent — NL to Pipeline](#7-ai-agent--nl-to-pipeline)
8. [Multi-worker: Gunicorn e implicazioni](#8-multi-worker-gunicorn-e-implicazioni)
9. [Osservabilità: tracing, metriche, logging](#9-osservabilità-tracing-metriche-logging)
10. [Airflow e File-based XCom](#10-airflow-e-file-based-xcom)
11. [Security design](#11-security-design)
12. [Trade-off e debito tecnico noto](#12-trade-off-e-debito-tecnico-noto)

---

## 1. Visione d'insieme

La piattaforma implementa un **ETL decomponibile**: ogni operazione sui dati (estrai, pulisci,
valida, carica) è un servizio Flask indipendente. L'orchestrazione è separata dall'esecuzione.

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

**Principio guida**: nessun servizio conosce gli altri. Parlano solo attraverso il Preparator.

---

## 2. Flusso dati: Extract → Transform → Load

### Il dato non cambia formato tra i servizi

Tutti gli scambi di dati usano **Apache Arrow IPC Streaming** (non CSV, non JSON).

```
[CSV File]
    │
    ▼
[extract-csv-service]
    │  POST /extract-csv   body: {"file_path": "...", "dataset_name": "..."}
    │  ← risposta: binary Apache Arrow IPC stream (~165 KB per 500 righe HR)
    │
    ▼ ipc_bytes
[clean-nan-service]
    │  POST /clean-nan   body: <Arrow IPC>   header X-Params: {"strategy": "fill_median"}
    │  ← risposta: Arrow IPC trasformato
    │
    ▼ ipc_bytes
[load-data-service]
    │  POST /load-data   body: <Arrow IPC>   header X-Params: {"format": "parquet"}
    │  ← risposta: {"status": "ok", "path": "..."}  (JSON, unica eccezione)
    ▼
[File .parquet sul volume condiviso]
```

### Perché Arrow IPC invece di CSV/JSON?

| Criterio | CSV/JSON | Arrow IPC |
|---|---|---|
| Dimensione payload | 1x (baseline) | 0.3–0.6x (colonnare + compressione) |
| Parse overhead | Alto (deserializzazione row-by-row) | Quasi zero (zero-copy read) |
| Type safety | Nessuna (tutto stringa) | Schema tipizzato (int64, float64, date, ...) |
| Interoperabilità | Universale | Nativa in Python, Java, R, Rust, C++ |

L'overhead HTTP+serializzazione Arrow è ~5–15ms per hop, trascurabile rispetto al processing reale.

---

## 3. Protocollo di comunicazione inter-service

Esistono **4 pattern** distinti, a seconda del tipo di servizio:

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
<binary Arrow IPC trasformato>
```

I parametri vanno nell'header `X-Params` (JSON string) perché il body è già occupato dai dati.
Questo evita limiti di lunghezza URL e supporta oggetti annidati complessi.

### Pattern C — Multipart form (Join service)

Il join richiede due input indipendenti → `multipart/form-data` con i field `file1` e `file2`
(entrambi Arrow IPC) e i parametri (`join_key`, `join_type`) in `X-Params`.

### Pattern D — Arrow IPC in, JSON out (Load service)

Unica eccezione: `load-data-service` risponde con JSON (`{"status": "ok", "path": "...", "rows": N}`).
Il body in ingresso è sempre Arrow IPC; `X-Params` porta il formato di output (`csv`, `json`, `xlsx`, `parquet`).

---

## 4. Struttura interna di ogni microservizio

Ogni servizio segue il pattern Flask **app factory** con Blueprint:

```
services/<name>/
├── Dockerfile              FROM python:3.9-slim, gunicorn 4 workers
├── requirements.txt
├── run.py                  dev entry point (non usato in Docker)
└── app/
    ├── __init__.py         create_app(): factory, config, blueprint registration
    ├── routes.py           HTTP concerns: parse, dispatch, metrics, metadata
    └── <logic>.py          Pure data logic: no Flask imports
```

### Separazione delle responsabilità — routes.py vs logic.py

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

La logica pura in `logic.py` è **testabile senza HTTP scaffolding** — basta istanziare una
`pa.Table` e chiamare la funzione. I test unitari non usano mai `app.test_client()` per le
trasformazioni.

### App factory con hard limits

`create_app()` configura tre cose fondamentali: `MAX_CONTENT_LENGTH = 500 MB`, logging
JSON strutturato via `configure_service_logging()`, e handler JSON per 404/413 (invece
dell'HTML di default di Flask).

---

## 5. Preparator SDK — client-side orchestration

Il Preparator (`preparator/preparator_v4.py`) è il **layer client** che astrae tutta la comunicazione HTTP.
DAG Airflow, AI Agent, Streamlit: tutti chiamano esclusivamente il Preparator.

### Design decisions

#### Connection pooling (requests.Session)

```python
self.session = requests.Session()
# Un singolo Session riusa connessioni TCP tra chiamate consecutive
# → elimina il TCP handshake overhead su ogni hop della pipeline
```

#### Retry con backoff esponenziale

```python
retry_strategy = Retry(
    total=3,
    backoff_factor=0.5,       # sleep 0.5s, 1s, 2s tra i tentativi
    status_forcelist=[502, 503, 504],  # solo errori transitori
    allowed_methods=["POST", "GET"],
)
```

Solo errori **transitori** (gateway/timeout) fanno scattare il retry. Errori 4xx (bad request)
non vengono ritentati — sarebbero deterministi.

#### Timeout asimmetrico

```python
DEFAULT_TIMEOUT = (5, 300)
# connect_timeout=5s: il servizio deve rispondere subito al TCP connect
# read_timeout=300s:  ma l'elaborazione può durare fino a 5 minuti
# (es. LLM inference su dataset grande)
```

#### Correlation ID propagato su ogni chiamata

```python
self.correlation_id = correlation_id or str(uuid.uuid4())

# Ogni chiamata HTTP include:
headers={"X-Correlation-ID": self.correlation_id}
```

Un `correlation_id` per istanza Preparator → tutti gli hop di una singola pipeline execution
condividono lo stesso ID → i log di tutti i servizi sono collegabili.

#### Context manager

```python
with Preparator(config) as prep:
    ipc = prep.extract_csv(dataset_name="hr", file_path="...")
    ipc = prep.clean_nan(ipc, strategy="fill_median")
    prep.load_data(ipc, format="parquet")
# session.close() chiamato automaticamente in __exit__
```

---

## 6. Pipeline Compiler — parallelismo e DAG topologico

Il `PipelineCompiler` (`ai_agent/pipeline_compiler.py`) esegue pipeline YAML usando parallelismo
reale quando i passi non dipendono l'uno dall'altro.

### Da YAML a grafo

```yaml
steps:
  - id: extract          # nessun depends_on → layer 0
    service: extract_csv

  - id: quality          # dipende da extract → layer 1
    depends_on: [extract]
    service: data_quality

  - id: clean            # dipende da quality → layer 2
    depends_on: [quality]
    service: clean_nan

  - id: remove_cols      # dipende da quality (non da clean) → layer 2
    depends_on: [quality]
    service: delete_columns

  - id: save             # dipende da entrambi → layer 3
    depends_on: [clean, remove_cols]
    service: load_data
```

### Algoritmo di Kahn per layering topologico

```
Layer 0:  [extract]                        → eseguito first
Layer 1:  [quality]                        → parallel (1 step)
Layer 2:  [clean, remove_cols]             → PARALLEL EXECUTION ✓
Layer 3:  [save]                           → eseguito last
```

L'algoritmo calcola `in_degree` per ogni nodo; i nodi a grado zero formano il layer corrente,
dopo di che decrementa i gradi dei figli e ripete. Se il grafo ha un **ciclo** (es. A dipende da B che dipende da A), Kahn termina prima di
aver visitato tutti i nodi → `ValueError: cycle detected`. La validazione avviene prima
dell'esecuzione.

### Esecuzione parallela con ThreadPoolExecutor

```python
def _execute_layer(self, layer, outputs, prep):
    """Esegue tutti gli step del layer in parallelo."""
    with ThreadPoolExecutor(max_workers=len(layer)) as executor:
        futures = {
            executor.submit(self._execute_step, step, outputs, prep): step
            for step in layer
        }
        for future in as_completed(futures):
            step = futures[future]
            result = future.result()           # propaga eccezioni
            outputs[step["id"]] = result.data  # salva output per step successivi
```

**Thread safety**: ogni step scrive nel dict `outputs` con una chiave diversa (il proprio `id`).
Non c'è accesso condiviso allo stesso dato → nessuna race condition.

**Nota**: si usa `ThreadPoolExecutor` (thread, non processi) perché il bottleneck è I/O-bound
(HTTP calls), non CPU-bound. Il GIL di Python non è un problema qui — i thread cedono il GIL
durante `socket.send()` e `socket.recv()`.

### Dispatch registry — zero if/elif

Invece di:
```python
# ❌ fragile, da aggiornare ogni volta che si aggiunge un servizio
if step["service"] == "extract_csv":
    result = prep.extract_csv(...)
elif step["service"] == "clean_nan":
    result = prep.clean_nan(...)
elif ...
```

Il compiler usa un registro:
```python
# ✓ estendibile: aggiungere un servizio = aggiungere 1 entry al dict
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
User: "estrai hr_sample.csv, rimuovi outliers, salva in parquet"
         │
         ▼
  PipelineAgent.generate_pipeline()
         │
         ├─► Costruisce system prompt da service_registry.json
         │   (nomi, tipi, parametri obbligatori e opzionali di ogni servizio)
         │
         ├─► Chiama LLM (OpenAI GPT-4o-mini o locale HuggingFace)
         │
         ├─► Riceve YAML grezzo
         │
         └─► validate_pipeline(yaml)
               │
               ├─ Schema JSON validation (pipeline_schema.json)
               ├─ Service names esistenti in registry
               ├─ Parametri obbligatori presenti
               ├─ depends_on referenziano step validi
               └─ Nessun ciclo nel grafo
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
    # GPT-4o-mini via API (richiede OPENAI_API_KEY)

class LocalProvider(LLMProvider):
    # Chiama text-completion-llm-service (HuggingFace Llama 3.2 1B Instruct)
    # Utile per ambienti air-gapped o testing senza costi API
```

Selezione via env var: `LLM_PROVIDER=openai` (default) o `LLM_PROVIDER=local`.

---

## 8. Multi-worker: Gunicorn e implicazioni

### Configurazione

Ogni servizio Flask gira sotto **Gunicorn con 4 worker pre-fork**:

```
gunicorn --workers=4 --timeout=300 --bind=0.0.0.0:<port> app:create_app()
```

Eccezione: `text-completion-llm-service` usa **1 worker** (il modello HuggingFace ~1.5GB non
può essere replicato 4 volte in memoria).

### Il modello pre-fork di Gunicorn

Il master fork 4 processi worker indipendenti (spazi di memoria separati). Nginx/qualsiasi
reverse proxy fa round-robin tra loro. Il master gestisce segnali OS e restart automatico
di worker crashati.

### Implicazione #1 — Prometheus counters distribuiti

I counter Prometheus (`Counter(...)`) sono in memoria di processo. Ogni worker mantiene i
**propri** counter. Quando Prometheus scrapa `GET /metrics`, raggiunge un solo worker per request.

**Effetto pratico**: il counter `extract_csv_requests_total` che vedi in Prometheus è il valore
del singolo worker che ha risposto alla richiesta di scraping — tipicamente ~1/4 delle richieste
reali (con 4 worker e distribuzione round-robin).

**Workaround non implementato** (debito tecnico noto): `prometheus_client` supporta la
[modalità multiprocess](https://github.com/prometheus/client_python#multiprocess-mode-eg-gunicorn)
che aggrega i counter di tutti i worker tramite una directory condivisa:

```python
# Richiederebbe:
os.environ["PROMETHEUS_MULTIPROC_DIR"] = "/tmp/prometheus_multiproc"
from prometheus_client import CollectorRegistry, multiprocess
```

**Implicazione pratica per il monitoraggio**: i _trend_ e i _rate_ restano significativi e
affidabili per rilevare anomalie. I valori assoluti sono sottostimati.

### Implicazione #2 — Lazy loading per il modello LLM

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

Il modello viene caricato alla **prima richiesta**, non all'avvio del container. Questo permette
al container di diventare "healthy" in ~3s invece di 30+s. La prima richiesta avrà latenza più
alta (~15–30s cold start).

### Implicazione #3 — Gunicorn timeout e pipeline lunghe

```
gunicorn --timeout=300  # 5 minuti per request
```

La pipeline più lunga stimata è LLM inference su 500k righe (~4 min). Il Preparator SDK usa
`read_timeout=300s` di default. Se la pipeline supera i 5 minuti, Gunicorn uccide il worker
e il client riceve `502 Bad Gateway` → il retry strategy fa scattare il backoff.

---

## 9. Osservabilità: tracing, metriche, logging

### Correlation ID — tracing distribuito senza Jaeger

```
Preparator.__init__()
    │  genera UUID → self.correlation_id
    │
    ├─► POST /extract-csv    X-Correlation-ID: abc-123
    ├─► POST /clean-nan      X-Correlation-ID: abc-123
    ├─► POST /load-data      X-Correlation-ID: abc-123
```

Ogni servizio legge il correlation ID dall'header (o ne genera uno nuovo se assente) tramite
`get_correlation_id()` da `common/service_utils.py`, lo include nei log strutturati e lo
propaga nella response. Cerca il correlation ID nei log per **ricostruire l'intera trace**.

### Structured logging JSON

Ogni log line è un JSON su una riga con i campi: `timestamp`, `level`, `service`, `message`,
`correlation_id`, `dataset_name`. Filtrabile direttamente con `jq`:

```bash
docker compose logs | jq 'select(.correlation_id == "abc-123")'
```

### Metadata files per audit trail

Ogni servizio scrive un file JSON in `/app/data/<dataset_name>/metadata/` dopo ogni elaborazione:

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

Questi file sono a bassa latenza (scritti localmente sul volume condiviso) e permettono
di ricostruire cosa è successo a ogni step senza dipendere da sistemi esterni.

### Prometheus scraping

```
ogni 15 secondi
    Prometheus → GET /metrics su ogni servizio → salva counter in TSDB
    retention: 15 giorni
    storage: /etc/prometheus/data (volume etl-prometheus-data)
```

---

## 10. Airflow e File-based XCom

### Il problema degli XCom PostgreSQL con dati binari

Airflow di default usa PostgreSQL per passare dati tra task tramite XCom (cross-communication).
Il limite pratico di PostgreSQL per bytea è ~1MB; per dataset Arrow IPC da 50k righe si supera
facilmente questo limite.

### Soluzione: file-based XCom

```python
# airflow/dags/xcom_file_utils.py

def save_ipc_to_shared(ipc_data, dataset_name, step_name) -> str:
    """Salva Arrow IPC su /app/data/<ds>/xcom/<step>_<ts>_<uuid>.arrow"""
    path = f"/app/data/{dataset_name}/xcom/{step_name}_{ts}_{uuid}.arrow"
    with open(path, "wb") as f:
        f.write(ipc_data)
    return path  # ← torna solo il path (stringa, <100 bytes) via XCom Postgres

def load_ipc_from_shared(file_path) -> bytes:
    with open(file_path, "rb") as f:
        return f.read()
```

Il volume `etl-containers-shared-data` è montato in tutti i container (Airflow, tutti i
microservizi) a `/app/data` → i file scritti da un task sono leggibili dal task successivo.

```
 XCom Postgres:  task_A → task_B   =  "/app/data/hr/xcom/extract_20260301T184000Z_abc.arrow"
                                                   ↑
                                        stringa piccola → ok per Postgres
 File system:    task_A → task_B   = Arrow IPC binary (165 KB, 2 MB, 50 MB...)
                                                   ↑
                                        dati reali → ok per filesystem condiviso
```

---

## 11. Security design

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

Nessun servizio accetta path arbitrari. `os.path.realpath` risolve symlink e `..` prima
del check. Tentare `/etc/passwd` o `../../secrets` restituisce HTTP 400.

### API URL validation (extract-api-service)

Se `ALLOW_PRIVATE_API_URLS=false` (default), il servizio blocca URL verso:
- `localhost`, `127.*`, `10.*`, `192.168.*`, `172.16–31.*` (RFC 1918)
- Metadati cloud: `169.254.169.254` (AWS/GCP/Azure IMDS)

Previene SSRF (Server-Side Request Forgery) in ambienti Docker dove altri container sono
raggiungibili tramite DNS interno.

### SQL injection protection

```python
# Il servizio extract-sql non accetta query con statement multipli
if ";" in query.rstrip(";"):
    raise ValueError("Multiple SQL statements are not allowed")

# Whitelist dei prefissi ammessi
ALLOWED_PREFIXES = ("SELECT ", "WITH ")
if not query.strip().upper().startswith(ALLOWED_PREFIXES):
    raise ValueError("Only SELECT/WITH queries are allowed")
```

---

## 12. Trade-off e debito tecnico noto

### Decisioni consapevoli

| Decisione | Alternativa scartata | Motivazione |
|---|---|---|
| Arrow IPC via HTTP | gRPC + Protobuf | setup più semplice, nessun proto schema |
| Thread pool per parallelismo | asyncio | più leggibile, I/O-bound non beneficia di async |
| File-based XCom | Redis/Celery | nessuna dipendenza extra, usa il volume già presente |
| X-Params header | query params URL | supporta oggetti annidati, no limite lunghezza |
| `python:3.9-slim` | immagini più recenti | stabilità; Arrow 18.x supporta 3.9 |

### Debito tecnico noto

1. **Prometheus multi-process mode non attivata** — i counter di Gunicorn sono per-worker.
   I trend sono affidabili, i valori assoluti sottostimati di ~1/4x.

2. **Arrow → Pandas → Arrow** — la maggior parte dei transform converte in Pandas per il
   processing. Per dataset >1M righe conviene usare `pyarrow.compute` direttamente.
   Eccezione: `load-data-service` con Parquet scrive direttamente da Arrow (via `pyarrow.parquet`).

3. **Nessuna autenticazione sui microservizi** — aperti sulla rete Docker interna.
   Per produzione: API key header o mTLS tra servizi.

4. **Counter Prometheus reset al restart del container** — `restart: always` in docker-compose
   è un workaround per crash, ma azzera le metriche. Considera Pushgateway per metriche persistenti.

5. **No Kubernetes manifests** — solo Docker Compose. Per scaling orizzontale (più worker
   dello stesso servizio dietro un load balancer) serve una migrazione a K8s + Horizontal Pod
   Autoscaler.
