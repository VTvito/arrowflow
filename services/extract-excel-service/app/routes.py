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

from app.extract import process_excel

bp = Blueprint('extract-excel', __name__)
logger = logging.getLogger('extract-excel-service')

REQUEST_COUNTER, SUCCESS_COUNTER, ERROR_COUNTER = create_service_counters('extract_excel')
register_standard_endpoints(bp, 'extract-excel-service')


@bp.route('/extract-excel', methods=['POST'])
def extract_excel():
    """Extract data from an Excel file and return Arrow IPC."""
    start_time = time.time()
    correlation_id = get_correlation_id()
    try:
        REQUEST_COUNTER.inc()
        logger.info("Received /extract-excel request.", extra={"correlation_id": correlation_id})

        data = request.get_json()
        dataset_name = data.get('dataset_name')
        file_path = data.get('file_path')

        if not file_path:
            ERROR_COUNTER.inc()
            return jsonify({"status": "error", "message": "Parameter 'file_path' is required"}), 400

        if not dataset_name:
            ERROR_COUNTER.inc()
            return jsonify({"status": "error", "message": "Parameter 'dataset_name' is required"}), 400

        dataset_name = sanitize_dataset_name(dataset_name)
        logger.info(f"Extracting from Excel: {file_path} for dataset '{dataset_name}'",
                    extra={"correlation_id": correlation_id})

        arrow_table = process_excel(file_path)
        rows = arrow_table.num_rows
        cols = arrow_table.num_columns

        if rows == 0:
            logger.warning("Extracted Arrow table has no rows.", extra={"correlation_id": correlation_id})

        ipc_data = table_to_ipc(arrow_table)
        SUCCESS_COUNTER.inc()
        logger.info(
            f"Successfully extracted Excel with {rows} rows, {cols} cols for dataset '{dataset_name}'.",
            extra={"correlation_id": correlation_id, "dataset_name": dataset_name},
        )

        save_metadata("extract-excel", dataset_name, {
            "file_path": file_path, "rows_out": rows, "cols_out": cols,
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
        logger.exception("Error during /extract-excel processing.", extra={"correlation_id": correlation_id})
        return jsonify({"status": "error", "message": str(e)}), 500
