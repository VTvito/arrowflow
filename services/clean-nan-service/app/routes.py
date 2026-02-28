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

from app.clean import apply_transformations

bp = Blueprint('clean-nan', __name__)
logger = logging.getLogger('clean-nan-service')

REQUEST_COUNTER, SUCCESS_COUNTER, ERROR_COUNTER = create_service_counters('clean_nan')
register_standard_endpoints(bp, 'clean-nan-service')


@bp.route('/clean-nan', methods=['POST'])
def clean_nan():
    """Clean NaN values from Arrow IPC data using configurable strategies."""
    start_time = time.time()
    correlation_id = get_correlation_id()
    try:
        REQUEST_COUNTER.inc()

        params = parse_x_params()
        dataset_name = params.get('dataset_name')

        if not dataset_name:
            ERROR_COUNTER.inc()
            return jsonify({"status": "error", "message": "No dataset_name provided in X-Params"}), 400

        dataset_name = sanitize_dataset_name(dataset_name)

        ipc_data = request.get_data()
        if not ipc_data:
            ERROR_COUNTER.inc()
            return jsonify({"status": "error", "message": "No Arrow IPC data received"}), 400

        table_in = ipc_to_table(ipc_data)
        rows_in = table_in.num_rows
        cols_in = table_in.num_columns

        strategy = params.get('strategy', 'drop')
        fill_value = params.get('fill_value')
        target_columns = params.get('columns')

        cleaned_table, nulls_handled, total_null, total_cells = apply_transformations(
            table_in, strategy=strategy, fill_value=fill_value, columns=target_columns
        )
        rows_out = cleaned_table.num_rows
        cols_out = cleaned_table.num_columns

        out_ipc = table_to_ipc(cleaned_table)
        SUCCESS_COUNTER.inc()
        logger.info(
            f"Cleaned dataset '{dataset_name}': rows_in={rows_in}, rows_out={rows_out}, "
            f"nulls_handled={nulls_handled}, strategy={strategy}",
            extra={"correlation_id": correlation_id, "dataset_name": dataset_name},
        )

        save_metadata("clean-nan", dataset_name, {
            "rows_in": rows_in, "rows_out": rows_out,
            "cols_in": cols_in, "cols_out": cols_out,
            "total_cells": total_cells, "na_total": total_null,
            "na_handled": nulls_handled, "strategy": strategy,
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
        logger.exception("Error during /clean-nan processing.", extra={"correlation_id": correlation_id})
        return jsonify({"status": "error", "message": str(e)}), 500
