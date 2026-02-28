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

from app.join import join_datasets_logic

bp = Blueprint('join-datasets', __name__)
logger = logging.getLogger('join-datasets-service')

REQUEST_COUNTER, SUCCESS_COUNTER, ERROR_COUNTER = create_service_counters('join_datasets')
register_standard_endpoints(bp, 'join-datasets-service')


@bp.route('/join-datasets', methods=['POST'])
def join_datasets():
    """Join two Arrow IPC datasets via multipart form-data."""
    start_time = time.time()
    correlation_id = get_correlation_id()
    try:
        REQUEST_COUNTER.inc()
        logger.info("Received /join-datasets request (multipart).", extra={"correlation_id": correlation_id})

        params = parse_x_params()
        dataset_name = params.get('dataset_name')
        join_key = params.get('join_key')
        join_type = params.get('join_type')

        if not dataset_name:
            ERROR_COUNTER.inc()
            return jsonify({"status": "error", "message": "Parameter 'dataset_name' is required"}), 400

        dataset_name = sanitize_dataset_name(dataset_name)

        if not join_key or not join_type:
            ERROR_COUNTER.inc()
            return jsonify({"status": "error", "message": "Both join_key and join_type are required"}), 400

        logger.info(f"Joining datasets with join_key={join_key}, type={join_type}",
                    extra={"correlation_id": correlation_id})

        file1 = request.files.get('dataset1')
        file2 = request.files.get('dataset2')

        if not file1 or not file2:
            ERROR_COUNTER.inc()
            return jsonify({"status": "error", "message": "Both datasets must be provided"}), 400

        ipc_data1 = file1.read()
        ipc_data2 = file2.read()

        table1 = ipc_to_table(ipc_data1)
        table2 = ipc_to_table(ipc_data2)
        rows1, cols1 = table1.num_rows, table1.num_columns
        rows2, cols2 = table2.num_rows, table2.num_columns

        joined_table, (rows_out, cols_out) = join_datasets_logic(table1, table2, join_key, join_type)
        out_ipc = table_to_ipc(joined_table)

        SUCCESS_COUNTER.inc()
        logger.info(
            f"Joined data: join_key={join_key}, type={join_type}. Output rows={rows_out}, cols={cols_out}",
            extra={"correlation_id": correlation_id, "dataset_name": dataset_name},
        )

        save_metadata("join-datasets", dataset_name, {
            "join_key": join_key, "join_type": join_type,
            "rows_1": rows1, "cols_1": cols1,
            "rows_2": rows2, "cols_2": cols2,
            "rows_out": rows_out, "cols_out": cols_out,
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
        logger.exception("Error during /join-datasets processing.", extra={"correlation_id": correlation_id})
        return jsonify({"status": "error", "message": str(e)}), 500
