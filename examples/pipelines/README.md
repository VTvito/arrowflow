# Example Pipeline Definitions

Ready-to-use YAML pipeline definitions for the ETL Microservices Platform.

## Usage

### In Streamlit UI
1. Open http://localhost:8501
2. Go to the **YAML Editor** tab
3. Paste any of these YAML files
4. Click **Execute Pipeline**

### Via AI Agent
Ask the AI agent in natural language — it will generate similar YAML automatically:
- *"Load the HR dataset, check quality, remove outliers on salary, and save as CSV"*
- *"Extract e-commerce orders, validate completeness, clean missing prices, save as Parquet"*
- *"Fetch weather data from Open-Meteo for Rome, clean it, and save as Parquet"*

## Available Pipelines

| File | Description | Data Source |
|---|---|---|
| `hr_analytics.yaml` | HR People Analytics (6 steps) | Bundled CSV (`data/demo/hr_sample.csv`) |
| `ecommerce_analytics.yaml` | E-commerce order analytics (5 steps) | Bundled CSV (`data/demo/ecommerce_orders.csv`) |
| `weather_data.yaml` | Weather forecast (4 steps) | Open-Meteo API (live, no key) |

## Prerequisites

```bash
# Load demo datasets into containers (required for HR and e-commerce pipelines)
make demo-data
```

The weather pipeline works immediately — it pulls live data from the Open-Meteo API.
