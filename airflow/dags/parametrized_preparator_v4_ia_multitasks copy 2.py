from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from datetime import datetime
import json
import logging
from preparator.preparator_v4 import Preparator

# Configura il logger a livello INFO (puoi abbassarlo se non vuoi molti log)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
}

CONFIG_PATH = '/opt/airflow/preparator/services_config.json'
# Definiamo le regole per il check di qualità fuori dalla funzione
QUALITY_RULES = {
    "min_rows": 1,
    "check_null_ratio": True,
    "threshold_null_ratio": 0.5
}

def get_preparator() -> Preparator:
    """Crea un'istanza di Preparator caricando la configurazione dei servizi."""
    with open(CONFIG_PATH) as f:
        services_config = json.load(f)
    return Preparator(services_config)

with DAG(
    "parametrized_preparator_v4_ia_multitasks_v2_streamlined",
    default_args=default_args,
    schedule_interval=None,
    params={
        "dataset_name": Param("demo_dataset", type="string", description="Name of dataset"),
        "file_path": Param("", type="string", description="Path of dataset file like /app/data/<dataset_name>.*"),
        "output_format": Param("csv", type="string", description="Output format of the dataset (csv, xlsx, etc.)"),
        "column_to_complete": Param("review", type="string", description="Column to complete with text-generation AI system"),
        "missing_placeholder": Param("[MISSING]", type="string", description="Placeholder to search for in text")
    },
    catchup=False,
    render_template_as_native_obj=True,
) as dag:

    @task(task_id="extract", retries=2)
    def extract_task(**context) -> bytes:
        params = context['params']
        dataset_name = params["dataset_name"]
        # Se file_path è vuoto, usa il default basato sul dataset_name
        file_path = params.get("file_path") or f"/app/data/{dataset_name}.csv"
        logger.info(f"Extracting dataset '{dataset_name}' from file: {file_path}")
        prep = get_preparator()
        ipc_data = prep.extract_csv(dataset_name=dataset_name, file_path=file_path)
        return ipc_data

    @task(task_id="quality_check", retries=2)
    def quality_task(ipc_data: bytes, **context) -> bytes:
        params = context['params']
        dataset_name = params["dataset_name"]
        logger.info(f"Running quality check for dataset '{dataset_name}'")
        prep = get_preparator()
        dq_data = prep.check_quality(ipc_data, dataset_name, rules=QUALITY_RULES)
        return dq_data

    @task(task_id="load")
    def load_task(ipc_data: bytes, **context) -> None:
        params = context['params']
        dataset_name = params["dataset_name"]
        output_format = params["output_format"]
        logger.info(f"Loading data for dataset '{dataset_name}' in format '{output_format}'")
        prep = get_preparator()
        prep.load_data(ipc_data, format=output_format, dataset_name=dataset_name)
        logger.info(f"Pipeline completed for dataset '{dataset_name}'")


    extracted_data = extract_task()
    quality_checked = quality_task(extracted_data)
    load_task(quality_checked)