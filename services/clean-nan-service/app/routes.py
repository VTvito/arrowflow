import logging
from flask import Blueprint, jsonify, request, Response
from prometheus_client import Counter, generate_latest
import pyarrow as pa
import os
import time
import json
from datetime import datetime
from app.clean import apply_transformations
from common.arrow_utils import ipc_to_table, table_to_ipc
from common.json_utils import NpEncoder

bp = Blueprint('clean-nan', __name__)
logger = logging.getLogger('clean-nan-service')

REQUEST_COUNTER = Counter('clean_nan_requests_total', 'Total requests for the clean NaN service')
SUCCESS_COUNTER = Counter('clean_nan_success_total', 'Total successful requests for the clean NaN service')
ERROR_COUNTER = Counter('clean_nan_error_total', 'Total failed requests for the clean NaN service')

@bp.route('/clean-nan', methods=['POST'])
def clean_nan():
    start_time = time.time()
    try:
        REQUEST_COUNTER.inc()
        logger.info("Received /clean-nan request.")

        dataset_name = request.args.get('dataset_name', 'no_dataset')
        ipc_data = request.get_data()
        if not ipc_data:
            ERROR_COUNTER.inc()
            return jsonify({"status": "error", "message": "No Arrow IPC data received"}), 400

        # Convert Arrow IPC to Table
        table_in = ipc_to_table(ipc_data)
        rows_in = table_in.num_rows
        cols_in = table_in.num_columns

        # Count NaN (in Pandas, easier)
        df_in = table_in.to_pandas()
        total_cells = df_in.size
        total_null = df_in.isna().sum().sum()

        cleaned_table, removed_null_count = apply_transformations(table_in)  # returns (arrow_table, num_nan_removed)
        rows_out = cleaned_table.num_rows
        cols_out = cleaned_table.num_columns

        # Convert Table to Arrow IPC
        out_ipc = table_to_ipc(cleaned_table)
        SUCCESS_COUNTER.inc()
        logger.info(f"Cleaned dataset '{dataset_name}': rows_in={rows_in}, rows_out={rows_out}, null_removed={removed_null_count}")

        # Save metadata
        end_time = time.time()
        timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        dataset_folder = f"/app/data/{dataset_name}"
        os.makedirs(dataset_folder, exist_ok=True)
        metadata_dir = os.path.join(dataset_folder, "metadata")
        os.makedirs(metadata_dir, exist_ok=True)
        metadata_path = os.path.join(metadata_dir, f"metadata_clean_nan_{timestamp}.json")

        metadata = {
            "service": "clean-nan",
            "dataset_name": dataset_name,
            "rows_in": rows_in,
            "rows_out": rows_out,
            "cols_in": cols_in,
            "cols_out": cols_out,
            "na_removed": removed_null_count,
            "duration_sec": round(end_time - start_time, 3),
            "timestamp": timestamp
        }

        with open(metadata_path, "w") as f:
            json.dump(metadata, f, cls=NpEncoder, indent=2)
        logger.info(f"Metadata saved to {metadata_path}")

        return Response(out_ipc, mimetype="application/vnd.apache.arrow.stream"), 200

    except Exception as e:
        ERROR_COUNTER.inc()
        logger.exception("Error during /clean-nan processing.")
        return jsonify({"status": "error", "message": str(e)}), 500

@bp.route('/metrics', methods=['GET'])
def metrics():
    return Response(generate_latest(), mimetype="text/plain")