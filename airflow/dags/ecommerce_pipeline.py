"""
E-commerce Orders Pipeline — Demo Use Case

ETL pipeline for e-commerce order analytics.
Uses the bundled demo dataset (ecommerce_orders.csv).

Pipeline steps:
  1. Extract CSV from shared volume
  2. Data Quality checks (min rows, null ratio, completeness)
  3. Outlier detection on TotalAmount (z-score)
  4. Clean NaN (fill with median for numeric columns)
  5. Load final dataset

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

logger = logging.getLogger("ecommerce_pipeline")

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
}

CONFIG_PATH = "/opt/airflow/preparator/services_config.json"

with DAG(
    "ecommerce_pipeline",
    default_args=default_args,
    schedule_interval=None,
    description="E-commerce order analytics ETL pipeline (demo dataset)",
    tags=["ecommerce", "analytics", "etl", "demo"],
    params={
        "dataset_name": Param("ecommerce_demo", type="string", description="Dataset identifier"),
        "file_path": Param(
            "/app/data/ecommerce_demo/data.csv",
            type="string",
            description="Path to e-commerce CSV file on shared volume",
        ),
        "output_format": Param(
            "csv", type="string",
            enum=["csv", "xlsx", "json", "parquet"],
            description="Output format",
        ),
        "z_threshold": Param(3.0, type="number", description="Z-score threshold for outlier detection"),
        "use_file_xcom": Param(True, type="boolean", description="Use file-based XCom for large datasets"),
    },
) as dag:

    @task.python
    def extract(params: dict) -> str:
        """Extract e-commerce orders from CSV."""
        with open(CONFIG_PATH) as f:
            services_config = json.load(f)

        dataset_name = params["dataset_name"]
        file_path = params["file_path"]

        with Preparator(services_config) as prep:
            ipc_data = prep.extract_csv(dataset_name=dataset_name, file_path=file_path)

        logger.info(f"Extracted {len(ipc_data)} bytes from {file_path}")

        if params.get("use_file_xcom", True):
            return save_ipc_to_shared(ipc_data, dataset_name, "extract")
        return ipc_data.hex()

    @task.python
    def quality_check(data_ref: str, params: dict) -> str:
        """Run data quality checks: min rows, null ratio, completeness."""
        with open(CONFIG_PATH) as f:
            services_config = json.load(f)

        dataset_name = params["dataset_name"]
        use_file = params.get("use_file_xcom", True)

        ipc_data = load_ipc_from_shared(data_ref) if use_file else bytes.fromhex(data_ref)

        rules = {
            "min_rows": 10,
            "check_null_ratio": True,
            "threshold_null_ratio": 0.3,
            "check_duplicates": True,
            "check_completeness": True,
        }

        with Preparator(services_config) as prep:
            result_data = prep.check_quality(ipc_data, dataset_name=dataset_name, rules=rules)

        logger.info(f"Quality check passed for {dataset_name}")

        if use_file:
            return save_ipc_to_shared(result_data, dataset_name, "quality")
        return result_data.hex()

    @task.python
    def detect_outliers(data_ref: str, params: dict) -> str:
        """Detect and remove outliers on TotalAmount using z-score."""
        with open(CONFIG_PATH) as f:
            services_config = json.load(f)

        dataset_name = params["dataset_name"]
        use_file = params.get("use_file_xcom", True)
        z_threshold = params.get("z_threshold", 3.0)

        ipc_data = load_ipc_from_shared(data_ref) if use_file else bytes.fromhex(data_ref)

        with Preparator(services_config) as prep:
            result_data = prep.detect_outliers(
                ipc_data,
                dataset_name=dataset_name,
                column="TotalAmount",
                z_threshold=z_threshold,
            )

        logger.info(f"Outlier detection completed on TotalAmount (z={z_threshold})")

        if use_file:
            return save_ipc_to_shared(result_data, dataset_name, "outliers")
        return result_data.hex()

    @task.python
    def clean_nulls(data_ref: str, params: dict) -> str:
        """Fill NaN values with median for numeric columns."""
        with open(CONFIG_PATH) as f:
            services_config = json.load(f)

        dataset_name = params["dataset_name"]
        use_file = params.get("use_file_xcom", True)

        ipc_data = load_ipc_from_shared(data_ref) if use_file else bytes.fromhex(data_ref)

        with Preparator(services_config) as prep:
            result_data = prep.clean_nan(
                ipc_data,
                dataset_name=dataset_name,
                strategy="fill_median",
                columns=["Quantity", "UnitPrice", "Discount", "TotalAmount"],
            )

        logger.info(f"NaN cleaning (fill_median) completed for {dataset_name}")

        if use_file:
            return save_ipc_to_shared(result_data, dataset_name, "clean")
        return result_data.hex()

    @task.python
    def load_output(data_ref: str, params: dict):
        """Save final dataset to disk."""
        with open(CONFIG_PATH) as f:
            services_config = json.load(f)

        dataset_name = params["dataset_name"]
        output_format = params.get("output_format", "csv")
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
    extracted = extract()
    quality_checked = quality_check(extracted)
    outliers_removed = detect_outliers(quality_checked)
    cleaned = clean_nulls(outliers_removed)
    load_output(cleaned)
