import logging
from flask import Blueprint, jsonify, request, Response
from prometheus_client import Counter, generate_latest
import pyarrow as pa
import os
import time
import json
from datetime import datetime

from common.arrow_utils import ipc_to_table, table_to_ipc
from common.json_utils import NpEncoder
from app.outliers import detect_and_remove_outliers

bp = Blueprint('outlier-detection', __name__)
logger = logging.getLogger('outlier-detection-service')

REQUEST_COUNTER = Counter('outlier_detection_requests_total', 'Total requests for outlier detection service')
SUCCESS_COUNTER = Counter('outlier_detection_success_total', 'Total successful requests for outlier detection service')
ERROR_COUNTER = Counter('outlier_detection_error_total', 'Total failed requests for outlier detection service')

@bp.route('/outlier-detection', methods=['POST'])
def outlier_detection():
    """
    POST /outlier-detection?dataset_name=...&column=...&z_threshold=3.0
    Body: Arrow IPC
    Output: Arrow IPC with outliers removed from that column.
    Also logs metadata in /app/data/<dataset_name>/metadata/
    """
    start_time = time.time()
    try:
        REQUEST_COUNTER.inc()
        dataset_name = request.args.get('dataset_name', 'no_dataset')
        column = request.args.get('column', None)
        z_threshold = float(request.args.get('z_threshold', 3.0))

        ipc_data = request.get_data()
        if not ipc_data:
            ERROR_COUNTER.inc()
            return jsonify({"status": "error", "message": "No Arrow IPC data"}), 400

        arrow_table = ipc_to_table(ipc_data)
        rows_in = arrow_table.num_rows
        cols_in = arrow_table.num_columns

        new_table, removed_count = detect_and_remove_outliers(arrow_table, column, z_threshold=z_threshold)
        rows_out = new_table.num_rows
        out_ipc = table_to_ipc(new_table)

        SUCCESS_COUNTER.inc()
        logger.info(f"Outlier detection done for '{dataset_name}', col='{column}', removed={removed_count}")

        # Metadata
        end_time = time.time()
        timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        dataset_folder = f"/app/data/{dataset_name}"
        os.makedirs(dataset_folder, exist_ok=True)
        meta_dir = os.path.join(dataset_folder, "metadata")
        os.makedirs(meta_dir, exist_ok=True)

        meta_file = os.path.join(meta_dir, f"metadata_outliers_{timestamp}.json")
        metadata = {
            "service_name": "outlier-detection",
            "dataset_name": dataset_name,
            "column": column,
            "z_threshold": z_threshold,
            "rows_in": rows_in,
            "rows_out": rows_out,
            "removed_outliers": int(removed_count),
            "duration_sec": round(end_time - start_time, 3),
            "timestamp": timestamp
        }
        with open(meta_file, "w") as f:
            json.dump(metadata, f, cls=NpEncoder, indent=2)

        return Response(out_ipc, mimetype="application/vnd.apache.arrow.stream"), 200

    except Exception as e:
        ERROR_COUNTER.inc()
        logger.exception("Error in outlier detection.")
        return jsonify({"status": "error", "message": str(e)}), 500

@bp.route('/metrics', methods=['GET'])
def metrics():
    return Response(generate_latest(), mimetype="text/plain")