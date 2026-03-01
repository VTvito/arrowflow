# Demo Guide — Come usare ArrowFlow

Guida pratica step-by-step. Spiega **cosa fa** la piattaforma, **quando usarla** e **come**
percorrerla nei tre modi disponibili: UI no-code, YAML editor, Python SDK.

---

## Perché ArrowFlow? — Il problema che risolve

Hai un dataset grezzo (CSV da un database HR, ordini da un e-commerce, dati meteo da un'API) e
devi farlo diventare un file pulito, validato, pronto per l'analisi o per un modello ML.

Il percorso tipico:
1. Scrivi uno script Python ad hoc
2. Lo script fa tutto: legge, pulisce, valida, salva — è un monolite fragile
3. Cambiano i requisiti → riscrivi lo script
4. Vuoi scalare su dataset più grandi → riscrivi ancora

**ArrowFlow separa ogni operazione in un servizio indipendente.** Puoi comporre i passi
come blocchi LEGO, cambiarne uno senza toccare gli altri, e scalare solo quello che è lento.

In più: descrivi quello che vuoi **in italiano o inglese**, e il sistema genera e lancia la
pipeline al posto tuo.

---

## Prerequisiti

Stack in esecuzione (se non è partito):

```bash
make quickstart   # prima volta: copia .env, build, start, carica demo data
# oppure, se già buildato:
make up
make demo-data    # carica hr_demo/data.csv e ecommerce_demo/data.csv nei container
```

Verifica che i servizi siano sani:

```bash
curl http://localhost:5001/health  # extract-csv-service
curl http://localhost:5002/health  # clean-nan-service
```

---

## Scenario A — Streamlit UI (zero codice)

**Quando usarlo:** vuoi esplorare la piattaforma o costruire una pipeline senza scrivere YAML.

### 1. Apri la UI

http://localhost:8501

Vedrai quattro tab: **Chat**, **YAML Editor**, **Service Catalog**, **Health Dashboard**.

### 2. Tab Chat — descrivi la pipeline in linguaggio naturale

Nella casella di testo digita qualcosa del tipo:

> *"Carica il dataset HR, controlla la qualità dei dati, rimuovi gli outlier sul salario mensile e salva come CSV"*

Il pannello mostra:
- Il YAML generato dall'AI agent
- Un pulsante **Execute**
- Il log in tempo reale dell'esecuzione step-by-step
- In fondo: anteprima dei dati risultanti e download

> **Nota:** per la generazione AI serve `OPENAI_API_KEY` nel `.env`. Senza chiave puoi
> comunque usare il YAML Editor con le pipeline pre-fatte.

### 3. Esegui e osserva

Clicca **Execute**. Vedrai i passi completarsi in sequenza (o in parallelo se indipendenti).
Al termine compare un'anteprima della tabella e il path del file salvato in `/app/data/`.

---

## Scenario B — YAML Editor (pipeline pre-fatta)

**Quando usarlo:** conosci già i passi che vuoi fare, non hai bisogno dell'AI.

### 1. Copia una pipeline di esempio

```bash
cat examples/pipelines/hr_analytics.yaml
```

### 2. Incolla nell'editor

Apri http://localhost:8501 → tab **YAML Editor** → incolla il contenuto.

Il validator mostra eventuali errori (servizio inesistente, parametro mancante, ciclo nel grafo)
prima ancora di eseguire.

### 3. Esegui

Clicca **Run Pipeline**. Ogni step mostra stato, durata e righe in/out.

La pipeline `hr_analytics.yaml` fa 6 passi in ~2-3 secondi su 501 righe:

```
extract     → 501 righe lette da /app/data/hr_demo/data.csv
quality     → 0 violazioni trovate
drop_cols   → 3 colonne rimosse (EmployeeCount, Over18, StandardHours)
outliers    → N outlier rimossi (MonthlyIncome, z_threshold=3.0)
clean       → righe con null droppate
save        → file CSV salvato in /app/data/hr_analytics/...
```

### 4. Pipeline disponibili in `examples/pipelines/`

| File | Cosa fa |
|---|---|
| `hr_analytics.yaml` | HR attrition: quality → drop cols → outliers → clean → CSV |
| `ecommerce_analytics.yaml` | Ordini e-commerce: quality → outliers → fill median → Parquet |
| `weather_data.yaml` | Meteo live (Open-Meteo, no API key) → quality → ffill → Parquet |

---

## Scenario C — Python SDK (scripting diretto)

**Quando usarlo:** integri ArrowFlow in un notebook, uno script batch, o un DAG personalizzato.

### Setup

```python
import json
from preparator.preparator_v4 import Preparator

with open("preparator/services_config.json") as f:
    config = json.load(f)
```

### Pipeline HR in ~10 righe

```python
with Preparator(config) as prep:
    # 1. Estrai
    ipc = prep.extract_csv(
        dataset_name="hr_demo",
        file_path="/app/data/hr_demo/data.csv"
    )

    # 2. Qualità
    ipc = prep.check_quality(ipc, dataset_name="hr_demo", rules={
        "min_rows": 10,
        "check_null_ratio": True,
        "threshold_null_ratio": 0.5,
        "check_duplicates": True,
    })

    # 3. Rimuovi colonne inutili
    ipc = prep.delete_columns(ipc, dataset_name="hr_demo",
                              columns=["EmployeeCount", "Over18", "StandardHours"])

    # 4. Rileva e rimuovi outlier sul salario
    ipc = prep.detect_outliers(ipc, dataset_name="hr_demo",
                               column="MonthlyIncome", z_threshold=3.0)

    # 5. Pulisci null
    ipc = prep.clean_nan(ipc, dataset_name="hr_demo", strategy="drop")

    # 6. Salva
    result = prep.load_data(ipc, dataset_name="hr_demo", format="csv")
    print(result)  # {"status": "ok", "path": "/app/data/hr_demo/...", "rows": ...}
```

> Ogni chiamata è HTTP verso il relativo microservizio. Il `Preparator` gestisce retry
> automatici (3 tentativi, backoff 0.5s) e propaga lo stesso `correlation_id` su tutti gli hop.

### Convertire il risultato in Pandas

```python
import pyarrow as pa

# ipc è bytes Arrow IPC — convertibile in qualsiasi momento
reader = pa.ipc.open_stream(ipc)
df = reader.read_all().to_pandas()
print(df.shape)
```

---

## Scenario D — Airflow DAG (scheduled / production)

**Quando usarlo:** vuoi schedulare la pipeline, notifiche in caso di fallimento, retry automatici,
history delle esecuzioni.

### 1. Apri Airflow

http://localhost:8080 — `admin` / `admin`

### 2. Attiva e lancia un DAG

Vai su **DAGs** → cerca `hr_analytics_pipeline` → toggle **On** → pulsante **Trigger DAG ▶**.

Clicca sulla run per vedere il grafo dei task e i log di ogni step.

### 3. Trigger con configurazione personalizzata

Clicca **Trigger DAG w/ config** e passa JSON:

```json
{
  "dataset_name": "hr_demo",
  "output_format": "parquet",
  "z_threshold": 2.5,
  "use_file_xcom": true
}
```

`use_file_xcom: true` fa sì che i dati Arrow tra i task vengano scritti sul volume condiviso
(invece di passare per PostgreSQL) — fondamentale per dataset >50k righe.

### DAG disponibili

| DAG | Schedule | Descrizione |
|---|---|---|
| `hr_analytics_pipeline` | manuale | HR attrition: 6 step, parametrizzato |
| `ecommerce_pipeline` | manuale | Ordini: 5 step, parametrizzato |
| `weather_api_pipeline` | `@hourly` | Live API → Parquet (no API key) |
| `parametrized_preparator_v4_quality` | manuale | Generic quality pipeline |
| `parametrized_preparator_v4_quality_join` | manuale | Pipeline con join di due dataset |

---

## Cosa osservare durante l'esecuzione

### Metriche in Grafana

http://localhost:3000 — `admin` / `change-me-strong-password`

Il dashboard **ETL Microservices — Monitoring Overview** mostra:
- Servizi UP/DOWN in tempo reale
- Request rate per servizio (req/s)
- Error % nelle ultime 5 minuti
- CPU e memoria dei container (cAdvisor)

Dopo aver lanciato una pipeline, i contatori `requests_total` e `success_total` salgono
nello stesso ordine in cui i servizi vengono chiamati.

### Log strutturati

```bash
# Tutti i log di una run specifica (correlation_id nel body della UI o nell'output SDK)
docker compose logs | grep '"correlation_id": "abc-123"'

# Oppure con jq (se disponibile)
docker compose logs 2>&1 | jq 'select(.correlation_id == "abc-123")'
```

### Metadata audit

Dopo ogni pipeline, ogni servizio scrive un file JSON in `/app/data/<dataset_name>/metadata/`:

```bash
docker exec extract-csv-service ls /app/data/hr_demo/metadata/
# metadata_extract_csv_20260301T184000Z.json
# metadata_clean_nan_20260301T184001Z.json
# ...
```

---

## Troubleshooting rapido

| Sintomo | Causa probabile | Fix |
|---|---|---|
| `curl /health` → connection refused | servizio non partito | `docker compose ps` + `docker compose up -d <service>` |
| Streamlit mostra "AI agent not available" | `OPENAI_API_KEY` mancante | aggiungi la chiave in `.env` + `docker compose restart streamlit-app` |
| Pipeline fallisce con "file not found" | demo data non caricata | `make demo-data` |
| Airflow task fail "No module named..." | container Airflow non aggiornato | `docker compose up -d --build airflow` |
| Grafana "No data" nei panel | servizi non ancora scrapati | attendi 15s o lancia una pipeline per generare traffico |
