import requests
import logging
import urllib.parse

class Preparator:
    def __init__(self, services_config):
        """
        services_config is a dict like:
        {
            "extract_csv": "http://extract-csv-service:5001/extract-csv",
        }
        """
        self.services = services_config
        self.logger = logging.getLogger("Preparator")
        self.session = requests.Session()

    def run_service_json_in_ipc_out(self, service_key, json_data):
        """
        Calls a service that accepts JSON input, returns Arrow IPC on success.
        """
        self.logger.info(f"Calling {service_key} with JSON data: {json_data}")
        url = self.services[service_key]
        resp = self.session.post(url, json=json_data)
        resp.raise_for_status()
        return resp.content  # Arrow IPC bytes
    
    def run_service_ipc_in_ipc_out(self, service_key, ipc_data, params=None):
        """
        Calls a service that accepts Arrow IPC and returns Arrow IPC.
        Optionally pass query parameters (dict) in `params`.
        """
        self.logger.info(f"Calling {service_key} with IPC data size={len(ipc_data)} bytes")
        url = self.services[service_key]

        if params:
            qs = urllib.parse.urlencode(params)  # e.g. {"dataset_name": "...", "format": "..."}
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
        Calls extract-csv-service with JSON: { "dataset_name": ..., "file_path": ... }
        Returns Arrow IPC.
        """
        return self.run_service_json_in_ipc_out("extract_csv", {
            "dataset_name": dataset_name,
            "file_path": file_path
        })

    def extract_excel(self, dataset_name, file_path):
        """
        Calls extract-excel-service with JSON.
        """
        return self.run_service_json_in_ipc_out("extract_excel", {
            "dataset_name": dataset_name,
            "file_path": file_path
        })

    def extract_api(self, dataset_name, api_url, api_params={}, auth_type=None, auth_value=None):
        """
        Calls extract-api-service with JSON, returning Arrow IPC.
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
        Calls extract-sql-service with JSON, returning Arrow IPC.
        """
        json_data = {
            "dataset_name": dataset_name,
            "db_url": db_url,
            "query": query
        }
        return self.run_service_json_in_ipc_out("extract_sql", json_data)

    ### Transform microservices

    def clean_nan(self, ipc_data, dataset_name="default_dataset"):
        """
        Calls clean-nan-service with Arrow IPC + query param dataset_name.
        """
        return self.run_service_ipc_in_ipc_out("clean_nan", ipc_data, params={"dataset_name": dataset_name})

    def delete_columns(self, ipc_data, columns, dataset_name="default_dataset"):
        """
        Calls delete-columns-service with Arrow IPC + query param columns and dataset_name.
        """
        params_dict = {
            "columns": ",".join(columns),
            "dataset_name": dataset_name
        }
        return self.run_service_ipc_in_ipc_out("delete_columns", ipc_data, params=params_dict)

    ### Load microservice

    def load_data(self, ipc_data, format='csv', dataset_name="default_dataset"):
        """
        Calls load-data-service with Arrow IPC + query param format & dataset_name.
        """
        return self.run_service_ipc_in_ipc_out("load", ipc_data, params={"format": format, "dataset_name": dataset_name})
