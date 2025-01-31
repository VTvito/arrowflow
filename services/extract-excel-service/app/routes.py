import logging
from flask import Blueprint, jsonify, request, Response
from common.arrow_utils import table_to_ipc
from prometheus_client import Counter, generate_latest
import os
import pyarrow as pa
import pandas as pd
from datetime import datetime
from app.extract import process_excel

bp = Blueprint('extract-excel', __name__)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(name)s %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger('extract-excel-service')

REQUEST_COUNTER = Counter('extract_excel_requests_total', 'Total requests for the extract Excel service')
SUCCESS_COUNTER = Counter('extract_excel_success_total', 'Total successful requests for the extract Excel service')
ERROR_COUNTER = Counter('extract_excel_error_total', 'Total failed requests for the extract Excel service')

@bp.route('/extract-excel', methods=['POST'])
def extract_excel():
    """
    API Endpoint to extract data from an Excel file and return Arrow IPC.
    Input: JSON with:
      - client_id (str)
      - file_path (str): path to the Excel file

    Output:
      - On success: Arrow IPC binary (application/vnd.apache.arrow.stream)
      - On error: JSON {status: error, message: ...}
    """
    try:
        REQUEST_COUNTER.inc()
        logger.info("Received /extract-excel request.")

        data = request.get_json()
        client_id = data.get('client_id', 'client_id')
        file_path = data.get('file_path')

        if not file_path:
            logger.error("Missing 'file_path' in request.")
            ERROR_COUNTER.inc()
            return jsonify({"status": "error", "message": "Parameter 'file_path' is required"}), 400

        logger.info(f"Client {client_id} extracting from {file_path}.")

        # Process file Excel in DataFrame
        df = process_excel(file_path)
        logger.info(f"Loaded Excel with {df.shape[0]} rows and {df.shape[1]} columns.")

        # Convert DataFrame in Arrow Table
        arrow_table = pa.Table.from_pandas(df)

        # Convert Arrow Table in IPC
        ipc_data = table_to_ipc(arrow_table)

        SUCCESS_COUNTER.inc()
        logger.info("Successfully extracted Excel data and converted to Arrow IPC.")

        return Response(ipc_data, mimetype="application/vnd.apache.arrow.stream"), 200

    except Exception as e:
        ERROR_COUNTER.inc()
        logger.exception("Error during /extract-excel processing.")
        return jsonify({"status": "error", "message": str(e)}), 500


@bp.route('/metrics', methods=['GET'])
def metrics():
    """
    Prometheus monitoring endpoint.
    """
    return Response(generate_latest(), mimetype="text/plain")