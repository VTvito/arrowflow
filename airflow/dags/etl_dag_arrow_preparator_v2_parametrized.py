# airflow/dags/etl_dag_preparator_v2.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import json
import os

# Import del Preparator (assicurati di avere PYTHONPATH settato correttamente
# e di montare la cartella /opt/airflow/preparator con docker-compose)
from preparator.preparator_v3 import Preparator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025,1,1),
    'retries': 1,
}

def run_pipeline(**kwargs):
    # Carichiamo i servizi dal JSON
    with open("/opt/airflow/preparator/services_config.json") as f:
        services_config = json.load(f)
    prep = Preparator(services_config)

    # Leggiamo dataset_name da dag_run.conf (oppure usiamo un default)
    dataset_name = kwargs.get('dag_run').conf.get('dataset_name', 'demo_dataset')

    # Se l'utente vuole passare un file path custom:
    file_path = kwargs.get('dag_run').conf.get('file_path', f"/app/data/{dataset_name}.csv")

    # sample pipeline
    ipc_data = prep.extract_csv(dataset_name=dataset_name, file_path=file_path)
    cleaned_data = prep.clean_nan(ipc_data, dataset_name=dataset_name)
    final_data = prep.load_data(cleaned_data, format="csv", dataset_name=dataset_name)

    print(f"Pipeline for dataset '{dataset_name}' completed, using file '{file_path}'.")

with DAG('etl_arrow_parametrized_preparator_v2', default_args=default_args, schedule_interval=None) as dag:
    task_run_pipeline = PythonOperator(
        task_id='run_pipeline',
        python_callable=run_pipeline,
        provide_context=True
    )