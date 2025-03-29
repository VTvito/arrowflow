import logging
import os
import json
import time
from datetime import datetime
from flask import Blueprint, jsonify, request, Response
from prometheus_client import Counter, generate_latest
import pyarrow as pa
import pandas as pd

from common.arrow_utils import ipc_to_table, table_to_ipc
from common.json_utils import NpEncoder
from app.completion import fill_missing_text

bp = Blueprint('text-completion-llm', __name__)
logger = logging.getLogger('text-completion-llm-service')

# Prometheus counters
REQUEST_COUNTER = Counter('text_completion_llm_requests_total', 'Total requests for the text completion service')
SUCCESS_COUNTER = Counter('text_completion_llm_success_total', 'Successful text completion requests')
ERROR_COUNTER = Counter('text_completion_llm_error_total', 'Failed text completion requests')

@bp.route('/text-completion-llm', methods=['POST'])
def text_completion():
    """
    API Endpoint (POST /text-completion-llm):
    - Body: Arrow IPC in binary
    - Headers: 'X-Params': JSON with the following fields:
             "dataset_name": (str) dataset name, for metadata saving
             "text_column": (str) name of the column containing text with placeholders
             "max_tokens": (int) maximum tokens to generate (default 20)
             "missing_placeholder": (str) placeholder to search for in the text
    Output:
      - Arrow IPC with the column where each occurrence of the placeholder is replaced by model-generated text.
      - Metadata is saved to /app/data/<dataset_name>/metadata/metadata_text_completion_llm_<timestamp>.json
    """
    start_time = time.time()
    try:
        REQUEST_COUNTER.inc()
        logger.info("Received /text-completion-llm request.")

        # 1) Read parameters from header 'X-Params'
        raw_header = request.headers.get('X-Params','{}')
        try:
            params = json.loads(raw_header)
        except json.JSONDecodeError:
            params = {}

        dataset_name = params.get("dataset_name")
        text_col = params.get("text_column")
        max_tokens = params.get("max_tokens", 20)
        missing_placeholder = params.get("missing_placeholder")
        
        if not dataset_name:
            ERROR_COUNTER.inc()
            logger.error("Parameter 'dataset_name' is required.")
            return jsonify({"status": "error", "message": "Parameter 'dataset_name' is required"}), 400

        if not text_col:
            ERROR_COUNTER.inc()
            logger.error("Parameter 'text_column' is required.")
            return jsonify({"status": "error", "message": "Parameter 'text_column' is required"}), 400
        
        if not missing_placeholder:
            ERROR_COUNTER.inc()
            logger.error("Parameter 'missing_placeholder' is required.")
            return jsonify({"status": "error", "message": "Parameter 'missing_placeholder' is required"}), 400
        
        # 2) Read Arrow IPC from the body
        ipc_data = request.get_data()
        if not ipc_data:
            ERROR_COUNTER.inc()
            logger.error("No Arrow IPC data received.")
            return jsonify({"status": "error", "message": "No Arrow IPC data"}), 400

        # 3) Convert to Pandas
        arrow_table = ipc_to_table(ipc_data)
        df = arrow_table.to_pandas()

        # 4) Execute the fill of the placeholder
        df_filled, total_completed, prompt_template = fill_missing_text(df, text_col, max_tokens=max_tokens,
                                                       missing_placeholder=missing_placeholder)

        # 5) Convert back to Arrow IPC
        out_table = pa.Table.from_pandas(df_filled)
        out_ipc = table_to_ipc(out_table)

        SUCCESS_COUNTER.inc()
        logger.info(f"Completed {total_completed} placeholders in column '{text_col}' (dataset '{dataset_name}')")

        # 6) Save metadata
        end_time = time.time()
        dataset_folder = f"/app/data/{dataset_name}"
        os.makedirs(dataset_folder, exist_ok=True)
        metadata_dir = os.path.join(dataset_folder, "metadata")
        os.makedirs(metadata_dir, exist_ok=True)
        timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        metadata_file = os.path.join(metadata_dir, f"metadata_text_completion_llm_{timestamp}.json")

        metadata = {
            "service_name": "text-completion-llm",
            "dataset_name": dataset_name,
            "text_column": text_col,
            "max_tokens": max_tokens,
            "missing_placeholder": missing_placeholder,
            "prompt_template": prompt_template,
            "placeholders_completed": total_completed,
            "duration_sec": round(end_time - start_time, 3),
            "timestamp": timestamp
        }

        with open(metadata_file, 'w') as f:
            json.dump(metadata, f, cls=NpEncoder, indent=2)
        logger.info(f"Metadata saved to {metadata_file}")

        # 7) Return the Arrow IPC output
        return Response(out_ipc, mimetype="application/vnd.apache.arrow.stream"), 200

    except Exception as e:
        ERROR_COUNTER.inc()
        logger.exception("Error in text-completion-llm service.")
        return jsonify({"status": "error", "message": str(e)}), 500

@bp.route('/metrics', methods=['GET'])
def metrics():
    """
    Endpoint for Prometheus metrics, similar to other microservices.
    """
    return Response(generate_latest(), mimetype="text/plain")