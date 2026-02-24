import logging
import os
import time
import json
from datetime import datetime
from flask import Blueprint, jsonify, request, Response
from prometheus_client import Counter, generate_latest
import pyarrow as pa

from common.arrow_utils import ipc_to_table, table_to_ipc
from common.json_utils import NpEncoder
from app.dq import basic_quality_checks

bp = Blueprint('data_quality', __name__)
logger = logging.getLogger('data-quality-service')

REQUEST_COUNTER = Counter('data_quality_requests_total', 'Total requests for the data quality service')
SUCCESS_COUNTER = Counter('data_quality_success_total', 'Total successful requests for the data quality service')
ERROR_COUNTER = Counter('data_quality_error_total', 'Total failed requests for the data quality service')

@bp.route('/data-quality', methods=['POST'])
def data_quality():
    """
    Data Quality API Endpoint:
      - Receives Arrow IPC data in the request body.
      - Optionally receives a JSON string of 'rules' in header.
      - Performs basic quality checks and logs the results to a JSON metadata file.
      - Returns the same Arrow IPC data unchanged.
    """
    start_time = time.time()
    try:
        REQUEST_COUNTER.inc()
        logger.info("Received /data-quality request.")

        # Read the custom header 'X-Params'
        raw_header = request.headers.get('X-Params', '{}')
        try:
            header_data = json.loads(raw_header)
        except json.JSONDecodeError:
            header_data = {}

        # Extract dataset_name and rules from dict
        dataset_name = header_data.get('dataset_name')
        rules = header_data.get('rules')

        if not dataset_name:
            ERROR_COUNTER.inc()
            logger.error("No dataset_name provided in header.")
            return jsonify({"status": "error", "message": "No dataset_name provided in header"}), 400
        
        if not rules:
            logger.info("No rules provided in header. Using default rules.")

        # Retrieve the Arrow IPC bytes from the request body.
        ipc_data = request.get_data()
        if not ipc_data:
            ERROR_COUNTER.inc()
            return jsonify({"status": "error", "message": "No Arrow IPC data received"}), 400

        # Deserialize the bytes into an Arrow Table.
        arrow_table = ipc_to_table(ipc_data)
        rows_in = arrow_table.num_rows
        cols_in = arrow_table.num_columns

        # Perform + quality checks
        dq_result = basic_quality_checks(arrow_table, rules)
        logger.info(f"Data quality checks completed for dataset '{dataset_name}' with {rows_in} rows and {cols_in} columns.")

        # Save a metadata file
        end_time = time.time()
        timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        
        dataset_folder = f"/app/data/{dataset_name}"
        os.makedirs(dataset_folder, exist_ok=True)
        meta_dir = os.path.join(dataset_folder, "metadata")
        os.makedirs(meta_dir, exist_ok=True)
        metadata_file = os.path.join(meta_dir, f"metadata_data_quality_{timestamp}.json")

        metadata = {
            "service_name": "data-quality",
            "dataset_name": dataset_name,
            "rows_in": rows_in,
            "cols_in": cols_in,
            "dq_checks": dq_result["checks"],
            "duration_sec": round(end_time - start_time, 3),
            "timestamp": timestamp
        }

        with open(metadata_file, "w") as f:
            json.dump(metadata, f, cls=NpEncoder, indent=2)
        logger.info(f"Data quality metadata saved to {metadata_file}")

        SUCCESS_COUNTER.inc()
        return Response(ipc_data, mimetype="application/vnd.apache.arrow.stream"), 200

    except Exception as e:
        ERROR_COUNTER.inc()
        logger.exception("Error in data quality service.")
        return jsonify({"status": "error", "message": str(e)}), 500

@bp.route('/metrics', methods=['GET'])
def metrics():
    return Response(generate_latest(), mimetype="text/plain")

@bp.route('/health', methods=['GET'])
def health():
    return jsonify({"status": "ok"}), 200
