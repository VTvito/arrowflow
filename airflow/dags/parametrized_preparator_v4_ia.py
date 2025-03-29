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
    "parametrized_preparator_v4_ia",
    default_args=default_args,
    schedule_interval=None,
    params={
        "dataset_name": Param("demo_dataset", type="string", description="Name of dataset"),
        "file_path": Param("", type="string", description="Path of dataset file like /app/data/<dataset_name>.*"),
        "output_format": Param("csv", type="string", description="Output format of the dataset (csv, xlsx, etc.)"),
        "column_to_complete": Param("", type="string", description="Column to complete with text-generation AI system"),
        "missing_placeholder": Param("[MISSING]", type="string", description="Placeholder to search for in text")
    },
) as dag:

    @task.python
    def run_pipeline(params: dict):
        # Load services from JSON
        with open(CONFIG_PATH) as f:
            services_config = json.load(f)

        prep = Preparator(services_config)

        # Get the parameters from the dict given to the task
        dataset_name = params.get("dataset_name")
        file_path = params.get("file_path")
        output_format = params.get("output_format")
        column_to_complete = params.get("column_to_complete")
        missing_placeholder = params.get("missing_placeholder")

        # Exec. of pipeline
        ipc_data = prep.extract_csv(dataset_name=dataset_name, file_path=file_path)
        dq_data = prep.check_quality(ipc_data, dataset_name, rules={"min_rows": 1,"check_null_ratio": True,"threshold_null_ratio": 0.5})
        completed_data = prep.text_completion_llm(
            dq_data,
            dataset_name=dataset_name,
            text_column=column_to_complete,
            max_tokens=3,
            missing_placeholder=missing_placeholder)
        final_data = prep.load_data(completed_data, format=output_format, dataset_name=dataset_name)

        print(f"Pipeline for dataset '{dataset_name}' completed, using file '{file_path}'.")

    run_pipeline()