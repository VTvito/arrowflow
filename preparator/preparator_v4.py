# This version of the Preparator class uses to pass the parameters to the microservices in the header of the HTTP request, in JSON format.
import requests
import logging
import json


class Preparator:
    def __init__(self, services_config):
        """
        Initialize the Preparator with a configuration dictionary mapping service keys to URLs (services_config.json)
          {
            "extract_csv": "http://extract-csv-service:5001/extract-csv",
            ...
          }
        """
        self.services = services_config
        self.logger = logging.getLogger("Preparator")
        self.session = requests.Session()

    # ----------------------------------------------------------------
    # For microservices that ACCEPT JSON and return Arrow IPC
    # ----------------------------------------------------------------

    def run_service_json_in_ipc_out(self, service_key, json_data):
        """
        Executes a POST request to a microservice that accepts JSON as input and returns Arrow IPC.
        Logs the service call and returns the response content.
        """
        self.logger.info(f"Calling {service_key} with JSON data: {json_data}")
        url = self.services[service_key]
        resp = self.session.post(url, json=json_data)
        resp.raise_for_status()
        return resp.content  # Arrow IPC in case of success

    # ----------------------------------------------------------------
    # For microservices that ACCEPT Arrow IPC in the body
    # and parameters in a header 'X-Params' (in JSON format)
    # ----------------------------------------------------------------
    def run_service_ipc_in_ipc_out_with_header(self, service_key, ipc_data, header_dict):
        """
        Executes a POST request to the microservice identified by `service_key`,
        sending `ipc_data` in the body and parameters (header_dict) in a header called 'X-Params' (as JSON).
        Logs the service call and returns the response content (expected to be Arrow IPC bytes).
        """
        header_json = json.dumps(header_dict)
        self.logger.info(f"Calling {service_key} with IPC data (size={len(ipc_data)}). Header: {header_json}")
        url = self.services[service_key]

        resp = self.session.post(
            url,
            data=ipc_data,  # Arrow IPC in body
            headers={
                "Content-Type": "application/vnd.apache.arrow.stream",
                "X-Params": header_json
            }
        )
        resp.raise_for_status()
        return resp.content  # Arrow IPC bytes in case of success

    # ================================================================
    # EXTRACTION
    # ================================================================
    def extract_csv(self, dataset_name, file_path):
        """
        'extract_csv' microservice that accepts JSON and returns Arrow IPC.
        """
        return self.run_service_json_in_ipc_out("extract_csv", {
            "dataset_name": dataset_name,
            "file_path": file_path
        })

    def extract_excel(self, dataset_name, file_path):
        """
        'extract_excel' microservice that accepts JSON and returns Arrow IPC.
        """
        return self.run_service_json_in_ipc_out("extract_excel", {
            "dataset_name": dataset_name,
            "file_path": file_path
        })

    def extract_api(self, dataset_name, api_url, api_params={}, auth_type=None, auth_value=None):
        """
        'extract_api' microservice that accepts JSON and returns Arrow IPC.
        """
        json_data = {
            "dataset_name": dataset_name,
            "api_url": api_url,
            "api_params": api_params
        }
        if auth_type is not None:
            json_data["auth_type"] = auth_type
        if auth_value is not None:
            json_data["auth_value"] = auth_value

        return self.run_service_json_in_ipc_out("extract_api", json_data)

    def extract_sql(self, dataset_name, db_url, query):
        """
        'extract_sql' microservice that accepts JSON and returns Arrow IPC.
        """
        json_data = {
            "dataset_name": dataset_name,
            "db_url": db_url,
            "query": query
        }
        return self.run_service_json_in_ipc_out("extract_sql", json_data)

    # ================================================================
    #   TRANSFORMATION
    # ================================================================
    def clean_nan(self, ipc_data, dataset_name="default_dataset"):
        """
        'clean_nan' microservice that receives Arrow data and parameters in the header
        """
        header_dict = {
            "dataset_name": dataset_name
        }
        return self.run_service_ipc_in_ipc_out_with_header("clean_nan", ipc_data, header_dict)

    def delete_columns(self, ipc_data, columns, dataset_name="default_dataset"):
        """
        'delete_columns' microservice with Arrow data in the body and parameters in the header
        """
        header_dict = {
            "dataset_name": dataset_name,
            "columns": columns  # passed as a list; the microservice handles it accordingly
        }
        return self.run_service_ipc_in_ipc_out_with_header("delete_columns", ipc_data, header_dict)

    def detect_outliers(self, ipc_data, dataset_name="default_dataset", column="value", z_threshold=3.0):
        """
        'outlier_detection' microservice
        """
        header_dict = {
            "dataset_name": dataset_name,
            "column": column,
            "z_threshold": z_threshold
        }
        return self.run_service_ipc_in_ipc_out_with_header("outlier_detection", ipc_data, header_dict)

    def check_quality(self, ipc_data, dataset_name="default_dataset", rules=None):
        """
        'data_quality' microservice
        """
        if rules is None:
            rules = {}
        header_dict = {
            "dataset_name": dataset_name,
            "rules": rules  # pass the dictionary of rules
        }
        return self.run_service_ipc_in_ipc_out_with_header("data_quality", ipc_data, header_dict)

    # Multi-Part HTTP request for join-datasets
    def join_datasets(self, ipc_data1, ipc_data2, dataset_name="default_dataset", join_key="id", join_type="inner"):
        """
        Calls join-datasets-service with a multipart form-data:
        - dataset1 (Arrow IPC)
        - dataset2 (Arrow IPC)
        Passes optional params in X-Params (JSON).
        Returns the joined Arrow IPC.
        """
        header_dict = {
            "dataset_name": dataset_name,
            "join_key": join_key,
            "join_type": join_type
        }
        x_params = json.dumps(header_dict)

        service_key = "join_datasets"
        url = self.services[service_key]

        self.logger.info(f"Calling {service_key} for join with 2 datasets. Key={join_key}, Type={join_type}")

        # Multipart request building 'datasetX' are the keys of data-form
        files = {
            'dataset1': ('dataset1.arrow', ipc_data1, 'application/vnd.apache.arrow.stream'),
            'dataset2': ('dataset2.arrow', ipc_data2, 'application/vnd.apache.arrow.stream')
        }
        headers = {
            "X-Params": x_params
        }
        resp = self.session.post(url, files=files, headers=headers)
        resp.raise_for_status()
        return resp.content  # Arrow IPC
    

    def text_completion_llm(self, ipc_data, dataset_name="default_dataset", text_column="column", max_tokens=None, missing_placeholder="[MISSING]"):
        """
        'text_completion_llm' microservice that uses Arrow IPC in the body and parameters in the header.
        Parameters are required and passed explicitly.
        """
        header_dict = {
            "dataset_name": dataset_name,
            "text_column": text_column,
            "max_tokens": max_tokens,
            "missing_placeholder": missing_placeholder
        }
        return self.run_service_ipc_in_ipc_out_with_header("text_completion_llm", ipc_data, header_dict)


    # ================================================================
    #   LOAD
    # ================================================================
    def load_data(self, ipc_data, format='csv', dataset_name="default_dataset"):
        """
        'load_data' microservice
        """
        header_dict = {
            "dataset_name": dataset_name,
            "format": format
        }
        return self.run_service_ipc_in_ipc_out_with_header("load_data", ipc_data, header_dict)