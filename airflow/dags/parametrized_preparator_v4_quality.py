from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from datetime import datetime
import json
import os
from preparator.preparator_v4 import Preparator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
}

CONFIG_PATH = '/opt/airflow/preparator/services_config.json'  # Path of .json mapping configuration file in Airflow container
with DAG(
    "parametrized_preparator_v4_quality",
    default_args=default_args,
    schedule_interval=None,
    params={
        "dataset_name": Param("demo_dataset", type="string", description="Name of dataset"),
        "file_path": Param("", type="string", description="Path of dataset file like /app/data/<dataset_name>.*" ),
        "output_format": Param("csv", type="string", description="Output format of the dataset (csv, xlsx, etc.)"),
    },
) as dag:

    @task.python
    def run_pipeline(params: dict):
        # Load services from JSON
        with open(CONFIG_PATH) as f:
            services_config = json.load(f)

        prep = Preparator(services_config)

        # Get the parameters from the dict gived to the task
        dataset_name = params.get("dataset_name", "demo_dataset")
        file_path = params.get("file_path")
        output_format = params.get("output_format", "csv")
        if not file_path:
            file_path = f"/app/data/{dataset_name}.csv"

        # Exec. of pipeline
        ipc_data = prep.extract_csv(dataset_name=dataset_name, file_path=file_path)
        dq_data = prep.check_quality(ipc_data, dataset_name, rules={"min_rows": 1,"check_null_ratio": True,"threshold_null_ratio": 0.5})
        outlier_data = prep.detect_outliers(dq_data, dataset_name, column="Height", z_threshold=3.0)
        cleaned_data = prep.clean_nan(outlier_data, dataset_name=dataset_name)
        final_data = prep.load_data(cleaned_data, format=output_format, dataset_name=dataset_name)

        print(f"Pipeline for dataset '{dataset_name}' completed, using file '{file_path}'.")

    run_pipeline()