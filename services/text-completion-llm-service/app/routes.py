import logging
import time

import pyarrow as pa
from common.arrow_utils import ipc_to_table, table_to_ipc
from common.path_utils import sanitize_dataset_name
from common.service_utils import (
    create_service_counters,
    get_correlation_id,
    parse_x_params,
    register_standard_endpoints,
    save_metadata,
)
from flask import Blueprint, Response, jsonify, request

from app.completion import fill_missing_text

bp = Blueprint('text-completion-llm', __name__)
logger = logging.getLogger('text-completion-llm-service')

REQUEST_COUNTER, SUCCESS_COUNTER, ERROR_COUNTER = create_service_counters('text_completion_llm')
register_standard_endpoints(bp, 'text-completion-llm-service')


@bp.route('/text-completion-llm', methods=['POST'])
def text_completion():
    """Replace placeholders in a text column with LLM-generated text."""
    start_time = time.time()
    correlation_id = get_correlation_id()
    try:
        REQUEST_COUNTER.inc()
        logger.info("Received /text-completion-llm request.", extra={"correlation_id": correlation_id})

        params = parse_x_params()
        dataset_name = params.get("dataset_name")
        text_col = params.get("text_column")
        max_tokens = params.get("max_tokens", 20)
        missing_placeholder = params.get("missing_placeholder")
        max_rows = params.get("max_rows")

        if not dataset_name:
            ERROR_COUNTER.inc()
            return jsonify({"status": "error", "message": "Parameter 'dataset_name' is required"}), 400

        dataset_name = sanitize_dataset_name(dataset_name)

        if not text_col:
            ERROR_COUNTER.inc()
            return jsonify({"status": "error", "message": "Parameter 'text_column' is required"}), 400

        if not missing_placeholder:
            ERROR_COUNTER.inc()
            return jsonify({"status": "error", "message": "Parameter 'missing_placeholder' is required"}), 400

        ipc_data = request.get_data()
        if not ipc_data:
            ERROR_COUNTER.inc()
            return jsonify({"status": "error", "message": "No Arrow IPC data"}), 400

        # Convert to Pandas for LLM processing
        arrow_table = ipc_to_table(ipc_data)
        df = arrow_table.to_pandas()

        df_filled, total_completed, prompt_template = fill_missing_text(
            df, text_col, max_tokens=max_tokens,
            missing_placeholder=missing_placeholder, max_rows=max_rows,
        )

        # Convert back to Arrow IPC
        out_table = pa.Table.from_pandas(df_filled)
        out_ipc = table_to_ipc(out_table)

        SUCCESS_COUNTER.inc()
        logger.info(
            f"Completed {total_completed} placeholders in column '{text_col}' (dataset '{dataset_name}')",
            extra={"correlation_id": correlation_id, "dataset_name": dataset_name},
        )

        save_metadata("text-completion-llm", dataset_name, {
            "text_column": text_col, "max_tokens": max_tokens,
            "missing_placeholder": missing_placeholder,
            "prompt_template": prompt_template,
            "placeholders_completed": total_completed,
        }, start_time)

        resp = Response(out_ipc, status=200, mimetype="application/vnd.apache.arrow.stream")
        resp.headers["X-Correlation-ID"] = correlation_id
        return resp

    except ValueError as ve:
        ERROR_COUNTER.inc()
        logger.error(str(ve), extra={"correlation_id": correlation_id})
        return jsonify({"status": "error", "message": str(ve)}), 400
    except Exception as e:
        ERROR_COUNTER.inc()
        logger.exception("Error in text-completion-llm service.", extra={"correlation_id": correlation_id})
        return jsonify({"status": "error", "message": str(e)}), 500
