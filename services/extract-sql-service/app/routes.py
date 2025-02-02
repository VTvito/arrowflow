import logging
from flask import Blueprint, jsonify, request, Response
from prometheus_client import Counter, generate_latest
import time
import json
import os
from datetime import datetime

from app.extract import extract_from_sql
from common.arrow_utils import table_to_ipc
from common.json_utils import NpEncoder

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
    Input (JSON):
    {
      "dataset_name": "...",
      "query": "SELECT ...",
      "db_url": "postgresql://user:pass@host/db"
    }
    Output: Arrow IPC
    """
    start_time = time.time()
    try:
        REQUEST_COUNTER.inc()
        logger.info("Received /extract-sql request.")

        data = request.get_json()
        dataset_name = data.get('dataset_name', 'no_dataset')
        query = data.get('query', 'SELECT * FROM table_name')
        db_url = data.get('db_url')

        if not db_url:
            ERROR_COUNTER.inc()
            return jsonify({"status": "error", "message": "Parameter 'db_url' is required"}), 400

        # Load into Arrow Table
        arrow_table = extract_from_sql(db_url, query)
        rows = arrow_table.num_rows
        cols = arrow_table.num_columns

        if rows == 0:
            logger.warning("Extracted Arrow table has no rows.")
        
        # Serialize to Arrow IPC
        ipc_data = table_to_ipc(arrow_table)

        SUCCESS_COUNTER.inc()
        logger.info(f"Extracted SQL data with {rows} rows, {cols} cols for dataset '{dataset_name}'.")

        # Metadata
        end_time = time.time()
        timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        dataset_folder = f"/app/data/{dataset_name}"
        os.makedirs(dataset_folder, exist_ok=True)
        metadata_dir = os.path.join(dataset_folder, "metadata")
        os.makedirs(metadata_dir, exist_ok=True)
        metadata_path = os.path.join(metadata_dir, f"metadata_extract_sql_{timestamp}.json")

        metadata = {
            "service_name": "extract-sql",
            "dataset_name": dataset_name,
            "db_url": db_url,
            "rows_out": rows,
            "cols_out": cols,
            "duration_sec": round(end_time - start_time, 3),
            "timestamp": timestamp
        }
        with open(metadata_path, "w") as f:
            json.dump(metadata, f, cls=NpEncoder, indent=2)

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