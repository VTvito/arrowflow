import logging
from flask import Blueprint, jsonify, request, Response
from app.read import load_csv_to_arrow
from prometheus_client import Counter, generate_latest
from common.arrow_utils import table_to_ipc
from common.json_utils import NpEncoder
import os
import time
import json
from datetime import datetime

bp = Blueprint('extract-csv', __name__)

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(name)s %(message)s'
)
logger = logging.getLogger('extract-csv-service')

# Monitoring counters
REQUEST_COUNTER = Counter('extract_csv_requests_total', 'Total requests for the extract CSV service')
SUCCESS_COUNTER = Counter('extract_csv_success_total', 'Total successful requests for the extract CSV service')
ERROR_COUNTER = Counter('extract_csv_error_total', 'Total failed requests for the extract CSV service')

@bp.route('/extract-csv', methods=['POST'])
def extract_csv():
    """
    API Endpoint to extract data from a CSV file and serialize it into Arrow IPC format.
    Input (JSON):
    {
      "dataset_name": "...",
      "file_path": "/app/data/somefile.csv"
    }
    Output: Arrow IPC
    """
    start_time = time.time()
    try:
        REQUEST_COUNTER.inc()
        logger.info("Received /extract-csv request.")

        data = request.get_json()
        dataset_name = data.get('dataset_name', 'no_dataset')
        file_path = data.get('file_path')
        if not file_path:
            logger.error("Missing 'file_path' in request.")
            ERROR_COUNTER.inc()
            return jsonify({"status": "error", "message": "Parameter 'file_path' is required"}), 400

        logger.info(f"Extracting dataset '{dataset_name}' from file {file_path}")

        # Load CSV into Arrow Table
        arrow_table = load_csv_to_arrow(file_path)
        rows = arrow_table.num_rows
        cols = arrow_table.num_columns

        if rows == 0:
            logger.warning("Extracted Arrow table has no rows.")

        # Serialize Arrow Table to IPC format
        ipc_data = table_to_ipc(arrow_table)

        SUCCESS_COUNTER.inc()
        logger.info(f"Successfully extracted and serialized data for dataset '{dataset_name}' with {rows} rows.")

        # Save metadata
        end_time = time.time()
        timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        dataset_folder = f"/app/data/{dataset_name}"
        os.makedirs(dataset_folder, exist_ok=True)
        metadata_dir = os.path.join(dataset_folder, "metadata")
        os.makedirs(metadata_dir, exist_ok=True)
        metadata_path = os.path.join(metadata_dir, f"metadata_extract_csv_{timestamp}.json")

        metadata = {
            "service_name": "extract-csv",
            "dataset_name": dataset_name,
            "file_path": file_path,
            "rows_out": rows,
            "cols_out": cols,
            "duration_sec": round(end_time - start_time, 3),
            "timestamp": timestamp
        }
        with open(metadata_path, "w") as f:
            json.dump(metadata, f, cls=NpEncoder, indent=2)

        return Response(ipc_data, mimetype="application/vnd.apache.arrow.stream"), 200

    except Exception as e:
        ERROR_COUNTER.inc()
        logger.exception("Error during /extract-csv processing.")
        return jsonify({"status": "error", "message": str(e)}), 500

@bp.route('/metrics', methods=['GET'])
def metrics():
    """
    Prometheus monitoring endpoint.
    """
    return Response(generate_latest(), mimetype="text/plain")