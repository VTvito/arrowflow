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
    "parametrized_preparator_v4_quality_join",
    default_args=default_args,
    schedule_interval=None,
    params={
        "dataset_name1": Param("demo_dataset", type="string", description="Name of dataset1"),
        "dataset_name2": Param("demo_dataset", type="string", description="Name of dataset2"),
        "file_path1": Param("", type="string", description="Path of dataset file like /app/data/<dataset_name>.*" ),
        "file_path2": Param("", type="string", description="Path of dataset file like /app/data/<dataset_name>.*" ),
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
        dataset_name1 = params.get("dataset_name1", "demo_dataset")
        file_path1 = params.get("file_path1")
        dataset_name2 = params.get("dataset_name2", "demo_dataset")
        file_path2 = params.get("file_path2")
        output_format = params.get("output_format", "csv")
        if not file_path1 or file_path2:
            file_path1 = f"/app/data/{dataset_name1}.csv"
            file_path2 = f"/app/data/{dataset_name2}.csv"
        joined_dataset_name = f"{dataset_name1}_{dataset_name2}"


        # Exec. of pipeline
        ipc_data1 = prep.extract_csv(dataset_name=dataset_name1, file_path=file_path1)
        dq_data1 = prep.check_quality(ipc_data1, dataset_name1, rules={"min_rows": 1,"check_null_ratio": True,"threshold_null_ratio": 0.5})
        outlier_data1 = prep.detect_outliers(dq_data1, dataset_name1, column="Height", z_threshold=3.0)
        cleaned_data1 = prep.clean_nan(outlier_data1, dataset_name=dataset_name1)

        print(f"Pipeline for dataset '{dataset_name1}' completed, using file '{file_path1}'.")

        ipc_data2 = prep.extract_csv(dataset_name=dataset_name2, file_path=file_path2)
        dq_data2 = prep.check_quality(ipc_data2, dataset_name2, rules={"min_rows": 2,"check_null_ratio": True,"threshold_null_ratio": 0.5})
        outlier_data2 = prep.detect_outliers(dq_data2, dataset_name2, column="Height", z_threshold=3.0)
        cleaned_data2 = prep.clean_nan(outlier_data2, dataset_name=dataset_name2)

        print(f"Pipeline for dataset '{dataset_name2}' completed, using file '{file_path2}'.")
    
        joined_data = prep.join_datasets(cleaned_data1, cleaned_data2, dataset_name=joined_dataset_name, join_key="id", join_type="inner")
        final_data = prep.load_data(joined_data, format=output_format, dataset_name=joined_dataset_name)

    run_pipeline()