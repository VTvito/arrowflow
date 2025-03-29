from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests


# Configurazione di base
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 11, 1),
    'retries': 1,
}

dag = DAG(
    'etl_arrow_xcom_old',
    default_args=default_args,
    description='ETL Pipeline with Apache Arrow',
    schedule_interval=None,  # Trigger manuale
)

def extract_csv(**kwargs):
    data = {
        "client_id": "airflow_client",
        "file_path": "/app/data/test_dataset.csv"  # Percorso all'interno del container
    }
    response = requests.post("http://extract-csv-service:5001/extract-csv", json=data)
    response.raise_for_status()
    # Salva i dati Arrow IPC in XCom come bytes
    kwargs['ti'].xcom_push(key='data_arrow', value=response.content)

def clean_nan(**kwargs):
    # Recupera i dati Arrow IPC da XCom
    data_arrow = kwargs['ti'].xcom_pull(key='data_arrow', task_ids='extract_csv')
    response = requests.post("http://clean-nan-service:5002/clean-nan", data=data_arrow, headers={"Content-Type": "application/vnd.apache.arrow.stream"})
    response.raise_for_status()
    # Salva i dati puliti in XCom o in una destinazione finale
    kwargs['ti'].xcom_push(key='cleaned_data_arrow', value=response.content)

def verify_cleaned_data(**kwargs):
    cleaned_data = kwargs['ti'].xcom_pull(key='cleaned_data_arrow', task_ids='clean_nan')
    # Puoi aggiungere logica per verificare i dati puliti, ad esempio salvarli su uno storage o analizzarli
    print(f"Cleaned data received, size: {len(cleaned_data)} bytes")

with DAG('etl_arrow_xcom', default_args=default_args, schedule_interval=None) as dag:
    task_extract = PythonOperator(
        task_id='extract_csv',
        python_callable=extract_csv,
        provide_context=True
    )

    task_clean = PythonOperator(
        task_id='clean_nan',
        python_callable=clean_nan,
        provide_context=True
    )

    task_verify = PythonOperator(
        task_id='verify_cleaned_data',
        python_callable=verify_cleaned_data,
        provide_context=True
    )

    task_extract >> task_clean >> task_verify