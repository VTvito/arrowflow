# This version of the Preparator class uses query parameters to pass the parameters to the microservices in the URL of the HTTP request.
import requests
import logging
import urllib.parse
import json

class Preparator:
    def __init__(self, services_config):
        """
        Initialize the Preparator with a configuration dictionary mapping service keys to URLs (services_config.json)
          {
              "extract_csv": "http://extract-csv-service:5001/extract-csv",
          }
        """

        self.services = services_config
        self.logger = logging.getLogger("Preparator")
        self.session = requests.Session()

    def run_service_json_in_ipc_out(self, service_key, json_data):
        """
        - Logs the service call
        - Makes an HTTP POST request to the service endpoint corresponding to service_key
        - Returns the response content (expected to be Arrow IPC bytes)

        An helper function that call a microservice that accept JSON in input and returns data in Arrow IPC format       
        Allow to easily call microservices without repeating the HTTP request logic.
        
        """
        self.logger.info(f"Calling {service_key} with JSON data: {json_data}")
        url = self.services[service_key]
        resp = self.session.post(url, json=json_data)
        resp.raise_for_status()
        return resp.content  # Arrow IPC bytes
    
    def run_service_ipc_in_ipc_out(self, service_key, ipc_data, params=None):
        """
        - Logs the service call        
        - Optionally appends query parameters to the URL if provided
        - Makes an HTTP POST request with the IPC data
        - Returns the response content (expected to be Arrow IPC bytes)

        Call a microservice that accepts Arrow IPC data as input and returns Arrow IPC data
        """
        self.logger.info(f"Calling {service_key} with IPC data size={len(ipc_data)} bytes")
        url = self.services[service_key]

        if params:
            qs = urllib.parse.urlencode(params)  # e.g., {"dataset_name": "...", "format": "..."}
            url = f"{url}?{qs}"

        resp = self.session.post(
            url, 
            data=ipc_data,
            headers={"Content-Type": "application/vnd.apache.arrow.stream"}
        )
        resp.raise_for_status()
        return resp.content  # Arrow IPC bytes

    ### Extraction microservices

    def extract_csv(self, dataset_name, file_path):
        """
        Call the extract-csv-service with JSON data:
          { "dataset_name": ..., "file_path": ... }
        Returns Arrow IPC
        """
        return self.run_service_json_in_ipc_out("extract_csv", {
            "dataset_name": dataset_name,
            "file_path": file_path
        })

    def extract_excel(self, dataset_name, file_path):
        """
        Call the extract-excel-service with JSON data
        """
        return self.run_service_json_in_ipc_out("extract_excel", {
            "dataset_name": dataset_name,
            "file_path": file_path
        })

    def extract_api(self, dataset_name, api_url, api_params={}, auth_type=None, auth_value=None):
        """
        Call the extract-api-service with JSON data and return Arrow IPC
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
        Call the extract-sql-service with JSON data and return Arrow IPC
        """
        json_data = {
            "dataset_name": dataset_name,
            "db_url": db_url,
            "query": query
        }
        return self.run_service_json_in_ipc_out("extract_sql", json_data)

    ### Transformation microservices

    def clean_nan(self, ipc_data, dataset_name="default_dataset"):
        """
        Call the clean-nan-service with Arrow IPC data and a query parameter for dataset_name
        """
        return self.run_service_ipc_in_ipc_out("clean_nan", ipc_data, params={"dataset_name": dataset_name})

    def delete_columns(self, ipc_data, columns, dataset_name="default_dataset"):
        """
        Call the delete-columns-service with Arrow IPC data and query parameters for columns and dataset_name
        """
        params_dict = {
            "columns": ",".join(columns),
            "dataset_name": dataset_name
        }
        return self.run_service_ipc_in_ipc_out("delete_columns", ipc_data, params=params_dict)
    
    def detect_outliers(self, ipc_data, dataset_name="default_dataset", column="value", z_threshold=3.0):
        """
        Calls outlier-detection-service with Arrow IPC and query params:
        dataset_name, column, z_threshold.
        Returns the cleaned Arrow IPC.
        """
        params = {
            "dataset_name": dataset_name,
            "column": column,
            "z_threshold": z_threshold
        }
        return self.run_service_ipc_in_ipc_out("outlier_detection", ipc_data, params=params)

    # Data Quality Assessment
    def check_quality(self, ipc_data, dataset_name="default_dataset", rules=None):
        """
        Call the data-quality-service with Arrow IPC data, along with a query parameter for dataset_name
        and an optional 'rules_json' parameter for quality rules
        """
        if rules is None:
            rules_str = "{}"
        else:
            rules_str = json.dumps(rules)

        params = {
            "dataset_name": dataset_name,
            "rules_json": rules_str
        }
        return self.run_service_ipc_in_ipc_out("data_quality", ipc_data, params=params)

    ### Load microservice

    def load_data(self, ipc_data, format='csv', dataset_name="default_dataset"):
        """
        Call the load-data-service with Arrow IPC data and query parameters for format and dataset_name
        """
        return self.run_service_ipc_in_ipc_out("load_data", ipc_data, params={"format": format, "dataset_name": dataset_name})