# This version of the Preparator class passes the parameters to the
# microservices in the header of the HTTP request, in JSON format.
import json
import logging
import uuid

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class Preparator:
    """
    Client SDK for the ETL microservices platform.
    Wraps HTTP calls to microservices with connection pooling, retries, and timeouts.
    Supports X-Correlation-ID propagation for end-to-end request tracing.
    """

    # Default timeouts: (connect_timeout, read_timeout) in seconds
    DEFAULT_TIMEOUT = (5, 300)

    def __init__(self, services_config, timeout=None, max_retries=3, correlation_id=None):
        """
        Initialize the Preparator with a configuration dictionary mapping service keys to URLs (services_config.json)
          {
            "extract_csv": "http://extract-csv-service:5001/extract-csv",
            ...
          }

        Args:
            services_config: dict mapping service keys to URLs
            timeout: tuple of (connect_timeout, read_timeout)
            max_retries: number of retries on 502/503/504
            correlation_id: optional correlation ID for tracing; auto-generated if None
        """
        self.services = services_config
        self.logger = logging.getLogger("Preparator")
        self.timeout = timeout or self.DEFAULT_TIMEOUT
        self.correlation_id = correlation_id or str(uuid.uuid4())

        # Configure session with retry logic
        self.session = requests.Session()
        retry_strategy = Retry(
            total=max_retries,
            backoff_factor=0.5,
            status_forcelist=[502, 503, 504],
            allowed_methods=["POST", "GET"],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def close(self):
        """Close the underlying HTTP session and release connections."""
        self.session.close()

    def _handle_error_response(self, resp, service_key):
        """Extract meaningful error message from service response."""
        try:
            resp.raise_for_status()
        except requests.HTTPError:
            # Try to parse the JSON error body from the service
            error_msg = f"Service '{service_key}' returned HTTP {resp.status_code}"
            try:
                error_body = resp.json()
                if "message" in error_body:
                    error_msg = f"[{service_key}] {error_body['message']}"
            except (ValueError, KeyError):
                if resp.text:
                    error_msg = f"[{service_key}] HTTP {resp.status_code}: {resp.text[:500]}"
            raise requests.HTTPError(error_msg, response=resp)

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
        resp = self.session.post(
            url, json=json_data, timeout=self.timeout,
            headers={"X-Correlation-ID": self.correlation_id},
        )
        self._handle_error_response(resp, service_key)
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
                "X-Params": header_json,
                "X-Correlation-ID": self.correlation_id,
            },
            timeout=self.timeout
        )
        self._handle_error_response(resp, service_key)
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

    def extract_api(self, dataset_name, api_url, api_params=None, auth_type=None, auth_value=None):
        """
        'extract_api' microservice that accepts JSON and returns Arrow IPC.
        """
        if api_params is None:
            api_params = {}
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
    def clean_nan(self, ipc_data, dataset_name="default_dataset", strategy="drop", fill_value=None, columns=None):
        """
        'clean_nan' microservice that receives Arrow data and parameters in the header.

        Args:
            ipc_data: Arrow IPC bytes.
            dataset_name: Logical dataset name.
            strategy: One of 'drop', 'fill_mean', 'fill_median', 'fill_mode',
                      'fill_value', 'ffill', 'bfill'.
            fill_value: Value to use when strategy='fill_value'.
            columns: Optional list of columns to apply the strategy to.
        """
        header_dict = {
            "dataset_name": dataset_name,
            "strategy": strategy,
        }
        if fill_value is not None:
            header_dict["fill_value"] = fill_value
        if columns is not None:
            header_dict["columns"] = columns
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

    def check_quality(self, ipc_data, dataset_name="default_dataset", rules=None, fail_on_errors=False):
        """
        'data_quality' microservice.

        Args:
            ipc_data: Arrow IPC bytes.
            dataset_name: Logical dataset name.
            rules: Dict of quality check rules.
            fail_on_errors: If True, the service returns HTTP 422 when any check fails,
                            which causes the pipeline to abort at this step.
        """
        if rules is None:
            rules = {}
        header_dict = {
            "dataset_name": dataset_name,
            "rules": rules,
            "fail_on_errors": fail_on_errors,
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
            "X-Params": x_params,
            "X-Correlation-ID": self.correlation_id,
        }
        resp = self.session.post(url, files=files, headers=headers, timeout=self.timeout)
        self._handle_error_response(resp, service_key)
        return resp.content  # Arrow IPC


    def text_completion_llm(
        self, ipc_data, dataset_name="default_dataset",
        text_column="column", max_tokens=None,
        missing_placeholder="[MISSING]",
    ):
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
