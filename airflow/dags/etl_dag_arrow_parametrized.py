# airflow/dags/etl_dag_preparator_v2.py
from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from datetime import datetime
import json
import os

# Import del Preparator (assicurati di avere PYTHONPATH settato correttamente
# e di montare la cartella /opt/airflow/preparator con docker-compose)
from preparator.preparator_v3 import Preparator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
}

with DAG(
    "etl_arrow_parametrized_preparator_v2",
    default_args=default_args,
    schedule_interval=None,
    params={
        "dataset_name": Param("demo_dataset", type="string"),
        "file_path": Param(
            "", 
            type="string", 
            description="Percorso del file CSV. Se lasciato vuoto verrà usato /app/data/<dataset_name>.csv"
        ),
    },
) as dag:

    @task.python
    def run_pipeline(params: dict):
        # Carichiamo i servizi dal JSON
        with open("/opt/airflow/preparator/services_config.json") as f:
            services_config = json.load(f)
        prep = Preparator(services_config)

        # Recuperiamo i parametri dal dict passato al task
        dataset_name = params.get("dataset_name", "demo_dataset")
        file_path = params.get("file_path")
        if not file_path:
            file_path = f"/app/data/{dataset_name}.csv"

        # Esecuzione della pipeline di esempio
        ipc_data = prep.extract_excel(dataset_name=dataset_name, file_path=file_path)
        cleaned_data = prep.clean_nan(ipc_data, dataset_name=dataset_name)
        final_data = prep.load_data(cleaned_data, format="csv", dataset_name=dataset_name)

        print(f"Pipeline for dataset '{dataset_name}' completed, using file '{file_path}'.")

    run_pipeline()