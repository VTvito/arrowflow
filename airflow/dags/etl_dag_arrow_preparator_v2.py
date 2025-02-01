from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from preparator.preparator_v2 import Preparator
import json

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 12, 1),
    'retries': 1,
}

CONFIG_PATH = '/opt/airflow/preparator/services_config.json'  # Path of .json config in Airflow container

def run_pipeline(**kwargs):
    
    # Loading configuration from JSON
    with open(CONFIG_PATH) as f:
        services_config = json.load(f)

    prep = Preparator(services_config)

    # example of pipeline
    ipc_data = prep.extract_excel(client_id="airflow_client", file_path="/app/data/test_db.xlsx")
    cleaned_data = prep.clean_nan(ipc_data)
    deleted_cols_data = prep.delete_columns(cleaned_data, ["id"])
    result_ipc = prep.load_data(deleted_cols_data, format='csv')
    print("Pipeline completed successfully")

with DAG('etl_arrow_preparator_v2', default_args=default_args, schedule_interval=None) as dag:
    task_run_pipeline = PythonOperator(
        task_id='run_pipeline',
        python_callable=run_pipeline,
        provide_context=True
    )