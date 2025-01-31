import logging
from flask import Blueprint, jsonify, request, Response
from app.extract import extract_from_api
from common.arrow_utils import table_to_ipc
import os
import pandas as pd
import pyarrow as pa
from datetime import datetime
from prometheus_client import Counter, generate_latest

bp = Blueprint('extract-api', __name__)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(name)s %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger('extract-api-service')

REQUEST_COUNTER = Counter('extract_api_requests_total', 'Total requests for the extract API service')
SUCCESS_COUNTER = Counter('extract_api_success_total', 'Total successful requests for the extract API service')
ERROR_COUNTER = Counter('extract_api_error_total', 'Total failed requests for the extract API service')

@bp.route('/extract-api', methods=['POST'])
def api_extraction():
    """
    Input:
      - client_id
      - api_url
      - api_params (dict)
      - auth_type (string)
      - auth_value (string)

    Output:
      - Arrow IPC data in case of success
      - JSON error if something goes wrong
    """
    try:
        REQUEST_COUNTER.inc()
        logger.info("Received /extract-api request.")

        data = request.get_json()
        client_id = data.get('client_id', 'client_id')
        api_url = data.get('api_url')
        api_params = data.get('api_params', {})
        auth_type = data.get('auth_type')
        auth_value = data.get('auth_value')

        if not api_url:
            logger.error("Missing 'api_url'.")
            ERROR_COUNTER.inc()
            return jsonify({"status": "error", "message": "Parameter 'api_url' is required"}), 400

        df = extract_from_api(api_url, api_params, auth_type, auth_value)
        logger.info(f"Extracted API data with {df.shape[0]} rows and {df.shape[1]} columns.")

        # Convert in Arrow IPC
        arrow_table = pa.Table.from_pandas(df)
        ipc_data = table_to_ipc(arrow_table)

        SUCCESS_COUNTER.inc()
        logger.info("Successfully extracted API data and converted to Arrow IPC.")

        return Response(ipc_data, mimetype="application/vnd.apache.arrow.stream"), 200

    except ValueError as ve:
        ERROR_COUNTER.inc()
        logger.error(str(ve))
        return jsonify({"status": "error", "message": str(ve)}), 400
    except Exception as e:
        ERROR_COUNTER.inc()
        logger.exception("Error during /extract-api processing.")
        return jsonify({"status": "error", "message": str(e)}), 500

@bp.route('/metrics', methods=['GET'])
def metrics():
    REQUEST_COUNTER.inc()
    return Response(generate_latest(), mimetype="text/plain")