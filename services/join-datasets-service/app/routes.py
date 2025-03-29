from flask import Blueprint, jsonify, request, Response
import logging
import time
import json
import os
from datetime import datetime
from app.join import join_datasets_logic
from prometheus_client import Counter, generate_latest
from common.arrow_utils import ipc_to_table, table_to_ipc
from common.json_utils import NpEncoder
import pyarrow as pa

bp = Blueprint('join-datasets', __name__)
logger = logging.getLogger('join-datasets-service')

REQUEST_COUNTER = Counter('join_datasets_requests_total', 'Total requests for the join datasets service')
SUCCESS_COUNTER = Counter('join_datasets_success_total', 'Total successful requests for the join datasets service')
ERROR_COUNTER = Counter('join_datasets_error_total', 'Total failed requests for the join datasets service')

@bp.route('/join-datasets', methods=['POST'])
def join_datasets():
    """
    Join Service:
      - Receives a multipart/form-data request with two files (dataset1, dataset2),
        each in Arrow IPC format.
      - Reads optional parameters (like join_key, join_type) from the header X-Params (JSON).
      - Performs the join and returns the result in Arrow IPC.
    """
    start_time = time.time()
    try:
        REQUEST_COUNTER.inc()
        logger.info("Received /join-datasets request (multipart).")

        # 1) parse header 'X-Params'
        raw_params = request.headers.get('X-Params', '{}')
        try:
            params = json.loads(raw_params)
        except json.JSONDecodeError:
            params = {}

        dataset_name = params.get('dataset_name')
        join_key = params.get('join_key')
        join_type = params.get('join_type')  # "inner", "left", "right", "outer"

        if not dataset_name:
            logger.error("Parameter 'dataset_name' is required.")
            ERROR_COUNTER.inc()
            return jsonify({"status": "error", "message": "Parameter 'dataset_name' is required"}), 400
        
        if not join_key or not join_type:
            logger.error("Both join_key and join_type are required.")
            ERROR_COUNTER.inc()
            return jsonify({"status": "error", "message": "Both join_key and join_type are required"}), 400
        
        logger.info(f"Joining datasets with join_key={join_key}, type={join_type}")

        # 2) Retrieve the two files from the multipart request
        file1 = request.files.get('dataset1')
        file2 = request.files.get('dataset2')

        if not file1 or not file2:
            logger.error("Both datasets (dataset1, dataset2) are required.")
            ERROR_COUNTER.inc()
            return jsonify({"status": "error", "message": "Both datasets must be provided"}), 400

        ipc_data1 = file1.read()
        ipc_data2 = file2.read()
        logger.info(f"Received dataset1 size={len(ipc_data1)}, dataset2 size={len(ipc_data2)} bytes")

        # 3) Convert both to Arrow Table
        table1 = ipc_to_table(ipc_data1)
        table2 = ipc_to_table(ipc_data2)
        rows1, cols1 = table1.num_rows, table1.num_columns
        rows2, cols2 = table2.num_rows, table2.num_columns

        # 4) Execute the join
        joined_table, (rows_out, cols_out) = join_datasets_logic(table1, table2, join_key, join_type)
        out_ipc = table_to_ipc(joined_table)

        SUCCESS_COUNTER.inc()
        logger.info(f"Joined data with join_key={join_key}, type={join_type}. Output rows={rows_out}, cols={cols_out}")

        # 5) Save metadata
        end_time = time.time()
        timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        dataset_folder = f"/app/data/{dataset_name}"
        os.makedirs(dataset_folder, exist_ok=True)
        meta_dir = os.path.join(dataset_folder, "metadata")
        os.makedirs(meta_dir, exist_ok=True)

        meta_file = os.path.join(meta_dir, f"metadata_join_{timestamp}.json")
        metadata = {
            "service_name": "join-datasets",
            "dataset_name": dataset_name,
            "join_key": join_key,
            "join_type": join_type,
            "rows_1": rows1,
            "cols_1": cols1,
            "rows_2": rows2,
            "cols_2": cols2,
            "rows_out": rows_out,
            "cols_out": cols_out,
            "duration_sec": round(end_time - start_time, 3),
            "timestamp": timestamp
        }
        with open(meta_file, "w") as f:
            json.dump(metadata, f, cls=NpEncoder, indent=2)

        return Response(out_ipc, mimetype="application/vnd.apache.arrow.stream"), 200

    except Exception as e:
        ERROR_COUNTER.inc()
        logger.exception("Error during /join-datasets processing.")
        return jsonify({"status": "error", "message": str(e)}), 500

@bp.route('/metrics', methods=['GET'])
def metrics():
    return Response(generate_latest(), mimetype="text/plain")