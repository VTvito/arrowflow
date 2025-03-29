import logging
from flask import Blueprint, jsonify, request, Response
from app.columns import drop_columns_arrow
from common.arrow_utils import ipc_to_table, table_to_ipc
from common.json_utils import NpEncoder
from prometheus_client import Counter, generate_latest
import os
import time
import json
from datetime import datetime

bp = Blueprint('delete-columns', __name__)

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger = logging.getLogger('delete-columns-service')

REQUEST_COUNTER = Counter('delete_columns_requests_total', 'Total requests for the delete columns service')
SUCCESS_COUNTER = Counter('delete_columns_success_total', 'Total successful requests for the delete columns service')
ERROR_COUNTER = Counter('delete_columns_error_total', 'Total failed requests for the delete columns service')

@bp.route('/delete-columns', methods=['POST'])
def delete_columns():
    """
    API Endpoint that receives Arrow IPC (Body) + header with columns to delete:
      - columns: comma-separated list of columns
      - dataset_name: param to identify dataset
    Returns Arrow IPC with those columns removed, and logs metadata.
    """
    start_time = time.time()
    try:
        REQUEST_COUNTER.inc()
        logger.info("Received /delete-columns request.")

        # Read the custom header 'X-Params'
        raw_header = request.headers.get('X-Params', '{}')
        try:
            header_data = json.loads(raw_header)
        except json.JSONDecodeError:
            header_data = {}

        # Now extract columns and dataset_name from that dict
        columns_str = header_data.get('columns', '')
        dataset_name = header_data.get('dataset_name')

        if not dataset_name:
            logger.error("No dataset_name provided in header.")
            ERROR_COUNTER.inc()
            return jsonify({"status": "error", "message": "No dataset_name provided in header"}), 400
        
        # Split and clean up column names
        columns_to_delete = [col.strip() for col in columns_str.split(',') if col.strip()]
        if not columns_to_delete:
            logger.error("No columns specified.")
            ERROR_COUNTER.inc()
            return jsonify({"status": "error", "message": "No columns specified"}), 400

        # Read Arrow IPC data
        ipc_data = request.get_data()
        if not ipc_data:
            logger.error("No Arrow IPC data received.")
            ERROR_COUNTER.inc()
            return jsonify({"status": "error", "message": "No Arrow IPC data"}), 400

        table_in = ipc_to_table(ipc_data)
        cols_in = table_in.num_columns

        # Drop columns
        updated_table, removed_cols_count = drop_columns_arrow(table_in, columns_to_delete)
        cols_out = updated_table.num_columns

        out_ipc = table_to_ipc(updated_table)

        SUCCESS_COUNTER.inc()
        logger.info(f"Dropped {removed_cols_count} columns from dataset '{dataset_name}'. Now has {cols_out} cols.")

        # Save metadata
        end_time = time.time()
        timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        dataset_folder = f"/app/data/{dataset_name}"
        os.makedirs(dataset_folder, exist_ok=True)
        metadata_dir = os.path.join(dataset_folder, "metadata")
        os.makedirs(metadata_dir, exist_ok=True)
        metadata_path = os.path.join(metadata_dir, f"metadata_delete_columns_{timestamp}.json")

        metadata = {
            "service_name": "delete-columns",
            "dataset_name": dataset_name,
            "cols_in": cols_in,
            "cols_out": cols_out,
            "columns_deleted": columns_to_delete,
            "removed_cols_count": removed_cols_count,
            "duration_sec": round(end_time - start_time,3),
            "timestamp": timestamp
        }
        with open(metadata_path, "w") as f:
            json.dump(metadata, f, cls=NpEncoder, indent=2)

        return Response(out_ipc, mimetype="application/vnd.apache.arrow.stream"), 200
    
    except Exception as e:
        ERROR_COUNTER.inc()
        logger.exception("Error during /delete-columns processing.")
        return jsonify({"status": "error", "message": str(e)}), 500

@bp.route('/metrics', methods=['GET'])
def metrics():
    return Response(generate_latest(), mimetype="text/plain")
