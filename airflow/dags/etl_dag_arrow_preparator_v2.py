from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from preparator_v2 import Preparator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 12, 1),
    'retries': 1,
}

def run_pipeline(**kwargs):
    # can be also an external .json file
    services_config = {
        "extract_csv": "http://extract-csv-service:5001/extract-csv",
        "clean_nan": "http://clean-nan-service:5002/clean-nan",
        "load": "http://load-data-service:5009/load-data",
        "extract_excel": "http://extract-excel-service:5007/extract-excel",
        "extract_api": "http://extract-api-service:5006/extract-api",
        "extract_sql": "http://extract-sql-service:5005/extract-sql",
        "delete_columns": "http://delete-columns-service:5004/delete-columns"
    }

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