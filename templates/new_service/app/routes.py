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

from app.logic import process_data  # TODO: rename to your logic function

bp = Blueprint('{{SERVICE_SLUG}}', __name__)
logger = logging.getLogger('{{SERVICE_NAME}}')

REQUEST_COUNTER, SUCCESS_COUNTER, ERROR_COUNTER = create_service_counters('{{SERVICE_SLUG}}')
register_standard_endpoints(bp, '{{SERVICE_NAME}}')


@bp.route('/{{ENDPOINT_NAME}}', methods=['POST'])
def handle_request():
    """
    Main endpoint — receives Arrow IPC data + X-Params header,
    applies business logic, returns Arrow IPC result.

    TODO: Customize this handler for your service.
    """
    start_time = time.time()
    correlation_id = get_correlation_id()
    try:
        REQUEST_COUNTER.inc()
        logger.info(
            "Received /{{ENDPOINT_NAME}} request.",
            extra={"correlation_id": correlation_id},
        )

        # ── Parse parameters from X-Params header ──
        params = parse_x_params()
        dataset_name = params.get('dataset_name')

        if dataset_name is None:
            ERROR_COUNTER.inc()
            return jsonify({"status": "error", "message": "No dataset_name provided in X-Params"}), 400

        dataset_name = sanitize_dataset_name(dataset_name)

        # ── Read Arrow IPC body ──
        ipc_data = request.get_data()
        if not ipc_data:
            ERROR_COUNTER.inc()
            return jsonify({"status": "error", "message": "No Arrow IPC data received"}), 400

        table_in = ipc_to_table(ipc_data)
        rows_in = table_in.num_rows
        cols_in = table_in.num_columns

        # ── Extract your custom parameters ──
        # TODO: read the params your service needs, e.g.:
        # my_param = params.get('my_param', 'default_value')

        # ── Call business logic (pure function, no Flask) ──
        result_table = process_data(table_in, params)
        rows_out = result_table.num_rows
        cols_out = result_table.num_columns

        # ── Serialize and respond ──
        out_ipc = table_to_ipc(result_table)
        SUCCESS_COUNTER.inc()

        logger.info(
            f"Processed dataset '{dataset_name}': rows_in={rows_in}, rows_out={rows_out}",
            extra={"correlation_id": correlation_id, "dataset_name": dataset_name},
        )

        save_metadata("{{SERVICE_SLUG}}", dataset_name, {
            "rows_in": rows_in, "rows_out": rows_out,
            "cols_in": cols_in, "cols_out": cols_out,
            # TODO: add service-specific stats
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
        logger.exception(
            "Error during /{{ENDPOINT_NAME}} processing.",
            extra={"correlation_id": correlation_id},
        )
        return jsonify({"status": "error", "message": str(e)}), 500
