import logging
from flask import Blueprint, jsonify, request, Response
from app.extract import extract_from_sql
import os
import pandas as pd
import pyarrow as pa
from prometheus_client import Counter, generate_latest
from common.arrow_utils import table_to_ipc

bp = Blueprint('extract-sql', __name__)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(name)s %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger('extract-sql-service')

REQUEST_COUNTER = Counter('extract_sql_requests_total', 'Total requests for the extract SQL service')
SUCCESS_COUNTER = Counter('extract_sql_success_total', 'Total successful requests for the extract SQL service')
ERROR_COUNTER = Counter('extract_sql_error_total', 'Total failed requests for the extract SQL service')

@bp.route('/extract-sql', methods=['POST'])
def extract_data():
    """
    Input:
      - client_id
      - query (SQL)
      - db_url
    Output:
      - Arrow IPC in caso di successo
      - JSON errore in caso di problemi
    """
    try:
        REQUEST_COUNTER.inc()
        logger.info("Received /extract-sql request.")

        data = request.get_json()
        client_id = data.get('client_id', 'client_id')
        query = data.get('query', 'SELECT * FROM table_name')
        db_url = data.get('db_url')

        if not db_url:
            logger.error("Missing 'db_url'.")
            ERROR_COUNTER.inc()
            return jsonify({"status": "error", "message": "Parameter 'db_url' is required"}), 400

        df = extract_from_sql(db_url, query)
        logger.info(f"Extracted SQL data with {df.shape[0]} rows and {df.shape[1]} columns.")

        # Convert in Arrow IPC
        arrow_table = pa.Table.from_pandas(df)
        ipc_data = table_to_ipc(arrow_table)

        SUCCESS_COUNTER.inc()
        logger.info("Successfully extracted SQL data and converted to Arrow IPC.")

        return Response(ipc_data, mimetype="application/vnd.apache.arrow.stream"), 200

    except ConnectionError as ce:
        ERROR_COUNTER.inc()
        logger.error(str(ce))
        return jsonify({"status": "error", "message": str(ce)}), 400
    except Exception as e:
        ERROR_COUNTER.inc()
        logger.exception("Error during /extract-sql processing.")
        return jsonify({"status": "error", "message": str(e)}), 500

@bp.route('/metrics', methods=['GET'])
def metrics():
    REQUEST_COUNTER.inc()
    return Response(generate_latest(), mimetype="text/plain")