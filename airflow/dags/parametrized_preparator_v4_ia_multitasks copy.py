from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from airflow.operators.python import get_current_context
from datetime import datetime
import json
import logging
import base64
from typing import Dict, Any
from preparator.preparator_v4 import Preparator

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
}

CONFIG_PATH = '/opt/airflow/preparator/services_config.json'

def get_preparator() -> Preparator:
    """Create a new Preparator instance with service configuration."""
    try:
        with open(CONFIG_PATH) as f:
            services_config = json.load(f)
        return Preparator(services_config)
    except Exception as e:
        logger.error(f"Failed to initialize Preparator: {str(e)}")
        raise

def encode_arrow(ipc_data: bytes) -> str:
    """Encode binary Arrow IPC data into a base64 string for XCom transfer."""
    return base64.b64encode(ipc_data).decode('utf-8')

def decode_arrow(encoded_str: str) -> bytes:
    """Decode base64 string back into binary Arrow IPC data."""
    return base64.b64decode(encoded_str.encode('utf-8'))

with DAG(
    "parametrized_preparator_v4_ia_multitasks_v0",
    default_args=default_args,
    schedule_interval=None,
    params={
        "dataset_name": Param("IMDB_with_missing", type="string", description="Name of dataset"),
        "file_path": Param("/app/data/IMDB_with_missing.csv", type="string", description="Path of dataset file like /app/data/<dataset_name>.*"),
        "output_format": Param("csv", type="string", description="Output format of the dataset (csv, xlsx, etc.)"),
        "column_to_complete": Param("review", type="string", description="Column to complete with text-generation AI system"),
        "missing_placeholder": Param("[MISSING]", type="string", description="Placeholder to search for in text")
    },
    catchup=False,
    render_template_as_native_obj=True,
) as dag:

    @task
    def extract_task(**context) -> str:
        """Extract data from CSV source and return Arrow IPC as base64 string."""
        try:
            params = context['params']
            logger.info(f"Extract Task - Received parameters: {params}")
            
            dataset_name = params.get("dataset_name")
            if not dataset_name:
                raise ValueError("dataset_name parameter is required")
            file_path = params.get("file_path")
            if not file_path:
                file_path = f"/app/data/{dataset_name}.csv"
            logger.info(f"Extract Task - Using file_path: {file_path}")

            prep = get_preparator()
            ipc_data = prep.extract_csv(dataset_name=dataset_name, file_path=file_path)
            logger.info(f"Extract Task - Extracted {len(ipc_data)} bytes")
            return encode_arrow(ipc_data)
        except Exception as e:
            logger.error(f"Error in extract_task: {str(e)}")
            raise

    @task
    def quality_task(encoded_ipc: str, **context) -> str:
        """Check data quality and return Arrow IPC as base64 string."""
        try:
            params = context['params']
            dataset_name = params.get("dataset_name")
            ipc_data = decode_arrow(encoded_ipc)
            prep = get_preparator()
            dq_data = prep.check_quality(
                ipc_data, 
                dataset_name,
                rules={"min_rows": 1, "check_null_ratio": True, "threshold_null_ratio": 0.5}
            )
            return encode_arrow(dq_data)
        except Exception as e:
            logger.error(f"Error in quality_task: {str(e)}")
            raise

    @task
    def load_task(encoded_ipc: str, **context) -> None:
        """Load processed data; receives Arrow IPC as base64 string."""
        try:
            params = context['params']
            dataset_name = params.get("dataset_name")
            output_format = params.get("output_format", "csv")
            ipc_data = decode_arrow(encoded_ipc)
            prep = get_preparator()
            prep.load_data(ipc_data, format=output_format, dataset_name=dataset_name)
            logger.info(f"Load Task - Pipeline completed for dataset '{dataset_name}'")
        except Exception as e:
            logger.error(f"Error in load_task: {str(e)}")
            raise

    # Chaining: ogni task scambia dati in formato base64 (string)
    extracted = extract_task()
    quality_checked = quality_task(extracted)
    load_task(quality_checked)
