import logging
import time

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

from app.columns import drop_columns_arrow

bp = Blueprint('delete-columns', __name__)
logger = logging.getLogger('delete-columns-service')

REQUEST_COUNTER, SUCCESS_COUNTER, ERROR_COUNTER = create_service_counters('delete_columns')
register_standard_endpoints(bp, 'delete-columns-service')


@bp.route('/delete-columns', methods=['POST'])
def delete_columns():
    """Remove specified columns from Arrow IPC data."""
    start_time = time.time()
    correlation_id = get_correlation_id()
    try:
        REQUEST_COUNTER.inc()
        logger.info("Received /delete-columns request.", extra={"correlation_id": correlation_id})

        header_data = parse_x_params()
        columns_raw = header_data.get('columns', '')
        dataset_name = header_data.get('dataset_name')

        if not dataset_name:
            ERROR_COUNTER.inc()
            return jsonify({"status": "error", "message": "No dataset_name provided in header"}), 400

        dataset_name = sanitize_dataset_name(dataset_name)

        # Support both list (from Preparator v4) and comma-separated string
        if isinstance(columns_raw, list):
            columns_to_delete = [col.strip() for col in columns_raw if col.strip()]
        else:
            columns_to_delete = [col.strip() for col in columns_raw.split(',') if col.strip()]
        if not columns_to_delete:
            ERROR_COUNTER.inc()
            return jsonify({"status": "error", "message": "No columns specified"}), 400

        ipc_data = request.get_data()
        if not ipc_data:
            ERROR_COUNTER.inc()
            return jsonify({"status": "error", "message": "No Arrow IPC data"}), 400

        table_in = ipc_to_table(ipc_data)
        cols_in = table_in.num_columns

        updated_table, removed_cols_count = drop_columns_arrow(table_in, columns_to_delete)
        cols_out = updated_table.num_columns

        out_ipc = table_to_ipc(updated_table)
        SUCCESS_COUNTER.inc()
        logger.info(
            f"Dropped {removed_cols_count} columns from dataset '{dataset_name}'. Now has {cols_out} cols.",
            extra={"correlation_id": correlation_id, "dataset_name": dataset_name},
        )

        save_metadata("delete-columns", dataset_name, {
            "cols_in": cols_in, "cols_out": cols_out,
            "columns_deleted": columns_to_delete,
            "removed_cols_count": removed_cols_count,
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
        logger.exception("Error during /delete-columns processing.", extra={"correlation_id": correlation_id})
        return jsonify({"status": "error", "message": str(e)}), 500
