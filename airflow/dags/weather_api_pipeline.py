"""
Weather API Pipeline — Demo Use Case

ETL pipeline that extracts weather data from a public API (Open-Meteo),
validates quality, cleans the data, and saves it.

Demonstrates the extract-api-service with a real public endpoint.
No API key required — Open-Meteo is free and open.

Pipeline steps:
  1. Extract data from Open-Meteo hourly forecast API
  2. Data Quality checks (min rows, completeness)
  3. Clean NaN values (forward fill — natural for time series)
  4. Load final dataset as Parquet

Supports file-based XCom for large datasets (>50k rows).
"""

import json
import logging
from datetime import datetime

from airflow.decorators import task
from airflow.models.param import Param
from xcom_file_utils import cleanup_xcom_files, load_ipc_from_shared, save_ipc_to_shared

from airflow import DAG
from preparator.preparator_v4 import Preparator

logger = logging.getLogger("weather_api_pipeline")

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
}

CONFIG_PATH = "/opt/airflow/preparator/services_config.json"

# Open-Meteo API — free, no key required
# Returns hourly weather data for a given location
DEFAULT_API_URL = "https://api.open-meteo.com/v1/forecast"
DEFAULT_API_PARAMS = {
    "latitude": 41.9028,          # Rome, Italy
    "longitude": 12.4964,
    "hourly": "temperature_2m,relative_humidity_2m,wind_speed_10m,precipitation",
    "timezone": "Europe/Rome",
    "forecast_days": 7,
}

with DAG(
    "weather_api_pipeline",
    default_args=default_args,
    schedule_interval=None,
    description="Weather data ETL pipeline (Open-Meteo public API — no key required)",
    tags=["weather", "api", "etl", "demo"],
    params={
        "dataset_name": Param("weather_rome", type="string", description="Dataset identifier"),
        "api_url": Param(
            DEFAULT_API_URL,
            type="string",
            description="Open-Meteo API URL",
        ),
        "latitude": Param(41.9028, type="number", description="Location latitude"),
        "longitude": Param(12.4964, type="number", description="Location longitude"),
        "forecast_days": Param(7, type="integer", description="Number of forecast days (1-16)"),
        "output_format": Param(
            "parquet", type="string",
            enum=["csv", "xlsx", "json", "parquet"],
            description="Output format",
        ),
        "use_file_xcom": Param(True, type="boolean", description="Use file-based XCom for large datasets"),
    },
) as dag:

    @task.python
    def extract_weather(params: dict) -> str:
        """Extract weather data from Open-Meteo API."""
        with open(CONFIG_PATH) as f:
            services_config = json.load(f)

        dataset_name = params["dataset_name"]
        api_url = params.get("api_url", DEFAULT_API_URL)

        api_params = {
            "latitude": params.get("latitude", 41.9028),
            "longitude": params.get("longitude", 12.4964),
            "hourly": "temperature_2m,relative_humidity_2m,wind_speed_10m,precipitation",
            "timezone": "Europe/Rome",
            "forecast_days": params.get("forecast_days", 7),
        }

        with Preparator(services_config) as prep:
            ipc_data = prep.extract_api(
                dataset_name=dataset_name,
                api_url=api_url,
                api_params=api_params,
            )

        logger.info(f"Extracted {len(ipc_data)} bytes from {api_url}")

        if params.get("use_file_xcom", True):
            return save_ipc_to_shared(ipc_data, dataset_name, "extract_api")
        return ipc_data.hex()

    @task.python
    def quality_check(data_ref: str, params: dict) -> str:
        """Run data quality checks: min rows, completeness."""
        with open(CONFIG_PATH) as f:
            services_config = json.load(f)

        dataset_name = params["dataset_name"]
        use_file = params.get("use_file_xcom", True)

        ipc_data = load_ipc_from_shared(data_ref) if use_file else bytes.fromhex(data_ref)

        rules = {
            "min_rows": 5,
            "check_null_ratio": True,
            "threshold_null_ratio": 0.5,
            "check_completeness": True,
        }

        with Preparator(services_config) as prep:
            result_data = prep.check_quality(ipc_data, dataset_name=dataset_name, rules=rules)

        logger.info(f"Quality check passed for {dataset_name}")

        if use_file:
            return save_ipc_to_shared(result_data, dataset_name, "quality")
        return result_data.hex()

    @task.python
    def clean_nulls(data_ref: str, params: dict) -> str:
        """Forward-fill NaN values (natural for time series data)."""
        with open(CONFIG_PATH) as f:
            services_config = json.load(f)

        dataset_name = params["dataset_name"]
        use_file = params.get("use_file_xcom", True)

        ipc_data = load_ipc_from_shared(data_ref) if use_file else bytes.fromhex(data_ref)

        with Preparator(services_config) as prep:
            result_data = prep.clean_nan(
                ipc_data,
                dataset_name=dataset_name,
                strategy="ffill",
            )

        logger.info(f"NaN cleaning (ffill) completed for {dataset_name}")

        if use_file:
            return save_ipc_to_shared(result_data, dataset_name, "clean")
        return result_data.hex()

    @task.python
    def load_output(data_ref: str, params: dict):
        """Save final dataset to disk."""
        with open(CONFIG_PATH) as f:
            services_config = json.load(f)

        dataset_name = params["dataset_name"]
        output_format = params.get("output_format", "parquet")
        use_file = params.get("use_file_xcom", True)

        ipc_data = load_ipc_from_shared(data_ref) if use_file else bytes.fromhex(data_ref)

        with Preparator(services_config) as prep:
            result = prep.load_data(ipc_data, format=output_format, dataset_name=dataset_name)

        logger.info(f"Pipeline output saved: {result}")

        if use_file:
            removed = cleanup_xcom_files(dataset_name)
            logger.info(f"Cleaned up {removed} temporary XCom files")

        return result

    # ── DAG wiring ──
    extracted = extract_weather()
    quality_checked = quality_check(extracted)
    cleaned = clean_nulls(quality_checked)
    load_output(cleaned)
