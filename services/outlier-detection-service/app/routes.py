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

from app.outliers import detect_and_remove_outliers

bp = Blueprint('outlier-detection', __name__)
logger = logging.getLogger('outlier-detection-service')

REQUEST_COUNTER, SUCCESS_COUNTER, ERROR_COUNTER = create_service_counters('outlier_detection')
register_standard_endpoints(bp, 'outlier-detection-service')


@bp.route('/outlier-detection', methods=['POST'])
def outlier_detection():
    """Detect and remove outliers from a column using z-score method."""
    start_time = time.time()
    correlation_id = get_correlation_id()
    try:
        REQUEST_COUNTER.inc()
        logger.info("Received /outlier-detection request.", extra={"correlation_id": correlation_id})

        header_data = parse_x_params()
        dataset_name = header_data.get('dataset_name')
        column = header_data.get('column', '')
        z_threshold = float(header_data.get('z_threshold', 3.0))

        if not dataset_name:
            ERROR_COUNTER.inc()
            return jsonify({"status": "error", "message": "No dataset_name provided in header"}), 400

        dataset_name = sanitize_dataset_name(dataset_name)

        if not column:
            ERROR_COUNTER.inc()
            return jsonify({"status": "error", "message": "No column provided in header"}), 400

        ipc_data = request.get_data()
        if not ipc_data:
            ERROR_COUNTER.inc()
            return jsonify({"status": "error", "message": "No Arrow IPC data"}), 400

        arrow_table = ipc_to_table(ipc_data)
        rows_in = arrow_table.num_rows

        new_table, removed_count = detect_and_remove_outliers(arrow_table, column, z_threshold=z_threshold)
        rows_out = new_table.num_rows
        out_ipc = table_to_ipc(new_table)

        SUCCESS_COUNTER.inc()
        logger.info(
            f"Outlier detection done for '{dataset_name}', col='{column}', removed={removed_count}",
            extra={"correlation_id": correlation_id, "dataset_name": dataset_name},
        )

        save_metadata("outlier-detection", dataset_name, {
            "column": column, "z_threshold": z_threshold,
            "rows_in": rows_in, "rows_out": rows_out,
            "removed_outliers": int(removed_count),
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
        logger.exception("Error in outlier detection.", extra={"correlation_id": correlation_id})
        return jsonify({"status": "error", "message": str(e)}), 500
