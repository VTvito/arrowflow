import logging
import time

from common.arrow_utils import table_to_ipc
from common.path_utils import sanitize_dataset_name
from common.service_utils import (
    create_service_counters,
    get_correlation_id,
    register_standard_endpoints,
    save_metadata,
)
from flask import Blueprint, Response, jsonify, request

from app.extract import extract_from_api

bp = Blueprint('extract-api', __name__)
logger = logging.getLogger('extract-api-service')

REQUEST_COUNTER, SUCCESS_COUNTER, ERROR_COUNTER = create_service_counters('extract_api')
register_standard_endpoints(bp, 'extract-api-service')


@bp.route('/extract-api', methods=['POST'])
def api_extraction():
    """Extract data from a REST API and return Arrow IPC."""
    start_time = time.time()
    correlation_id = get_correlation_id()
    try:
        REQUEST_COUNTER.inc()

        data = request.get_json(silent=True)
        if data is None:
            ERROR_COUNTER.inc()
            return jsonify({"status": "error", "message": "Invalid or missing JSON body"}), 400

        dataset_name = data.get('dataset_name')
        api_url = data.get('api_url')
        api_params = data.get('api_params', {})
        auth_type = data.get('auth_type')
        auth_value = data.get('auth_value')

        if not dataset_name:
            ERROR_COUNTER.inc()
            return jsonify({"status": "error", "message": "Parameter 'dataset_name' is required"}), 400

        dataset_name = sanitize_dataset_name(dataset_name)

        if not api_url:
            ERROR_COUNTER.inc()
            return jsonify({"status": "error", "message": "Parameter 'api_url' is required"}), 400

        arrow_table = extract_from_api(api_url, api_params, auth_type, auth_value)
        rows = arrow_table.num_rows
        cols = arrow_table.num_columns

        if rows == 0:
            logger.warning("Extracted Arrow table has no rows.", extra={"correlation_id": correlation_id})

        ipc_data = table_to_ipc(arrow_table)
        SUCCESS_COUNTER.inc()
        logger.info(
            f"Extracted API data with {rows} rows, {cols} cols for dataset '{dataset_name}'.",
            extra={"correlation_id": correlation_id, "dataset_name": dataset_name},
        )

        save_metadata("extract-api", dataset_name, {
            "api_url": api_url, "rows_out": rows, "cols_out": cols,
        }, start_time)

        resp = Response(ipc_data, status=200, mimetype="application/vnd.apache.arrow.stream")
        resp.headers["X-Correlation-ID"] = correlation_id
        return resp

    except ValueError as ve:
        ERROR_COUNTER.inc()
        logger.error(str(ve), extra={"correlation_id": correlation_id})
        return jsonify({"status": "error", "message": str(ve)}), 400
    except Exception as e:
        ERROR_COUNTER.inc()
        logger.exception("Error during /extract-api processing.", extra={"correlation_id": correlation_id})
        return jsonify({"status": "error", "message": str(e)}), 500
