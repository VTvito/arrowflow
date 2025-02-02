import logging
from flask import Blueprint, jsonify, request, Response
from prometheus_client import Counter, generate_latest
import os
import pandas as pd
import time
import json
from datetime import datetime

from app.extract import process_excel
from common.arrow_utils import table_to_ipc
from common.json_utils import NpEncoder

bp = Blueprint('extract-excel', __name__)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(name)s %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger('extract-excel-service')

REQUEST_COUNTER = Counter('extract_excel_requests_total', 'Total requests for extract Excel service')
SUCCESS_COUNTER = Counter('extract_excel_success_total', 'Total successful requests for extract Excel service')
ERROR_COUNTER = Counter('extract_excel_error_total', 'Total failed requests for extract Excel service')

@bp.route('/extract-excel', methods=['POST'])
def extract_excel():
    """
    Input: JSON with:
      - dataset_name
      - file_path (Excel .xls/.xlsx)
    Output: Arrow IPC
    """
    start_time = time.time()
    try:
        REQUEST_COUNTER.inc()
        logger.info("Received /extract-excel request.")

        data = request.get_json()
        dataset_name = data.get('dataset_name', 'no_dataset')
        file_path = data.get('file_path')
        if not file_path:
            logger.error("Missing 'file_path' param.")
            ERROR_COUNTER.inc()
            return jsonify({"status": "error", "message": "Parameter 'file_path' is required"}), 400

        logger.info(f"Extracting from Excel: {file_path} for dataset '{dataset_name}'")

        # Load Excel into Arrow Table
        arrow_table = process_excel(file_path)
        rows = arrow_table.num_rows
        cols = arrow_table.num_columns

        if rows == 0:
            logger.warning("Extracted Arrow table has no rows.")

        # Serialize Arrow Table to Arrow IPC
        ipc_data = table_to_ipc(arrow_table)

        SUCCESS_COUNTER.inc()
        logger.info(f"Successfully extracted Excel with {rows} rows, {cols} cols for dataset '{dataset_name}'.")

        # Metadata
        end_time = time.time()
        timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        dataset_folder = f"/app/data/{dataset_name}"
        os.makedirs(dataset_folder, exist_ok=True)
        metadata_dir = os.path.join(dataset_folder, "metadata")
        os.makedirs(metadata_dir, exist_ok=True)
        metadata_path = os.path.join(metadata_dir, f"metadata_extract_excel_{timestamp}.json")

        metadata = {
            "service_name": "extract-excel",
            "dataset_name": dataset_name,
            "file_path": file_path,
            "rows_out": rows,
            "cols_out": cols,
            "duration_sec": round(end_time - start_time,3),
            "timestamp": timestamp
        }
        with open(metadata_path, "w") as f:
            json.dump(metadata, f, cls=NpEncoder, indent=2)

        return Response(ipc_data, mimetype="application/vnd.apache.arrow.stream"), 200
    except Exception as e:
        ERROR_COUNTER.inc()
        logger.exception("Error during /extract-excel processing.")
        return jsonify({"status": "error", "message": str(e)}), 500

@bp.route('/metrics', methods=['GET'])
def metrics():
    return Response(generate_latest(), mimetype="text/plain")