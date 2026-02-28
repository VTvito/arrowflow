import logging
import time

from common.arrow_utils import ipc_to_table
from common.path_utils import sanitize_dataset_name
from common.service_utils import (
    create_service_counters,
    get_correlation_id,
    parse_x_params,
    register_standard_endpoints,
    save_metadata,
)
from flask import Blueprint, jsonify, request

from app.load import load_arrow_to_format, save_output_file

bp = Blueprint('load-data', __name__)
logger = logging.getLogger('load-data-service')

REQUEST_COUNTER, SUCCESS_COUNTER, ERROR_COUNTER = create_service_counters('load_data')
register_standard_endpoints(bp, 'load-data-service')


@bp.route('/load-data', methods=['POST'])
def load_data():
    """Save Arrow IPC data to a specified output format (csv/xlsx/json/parquet)."""
    start_time = time.time()
    correlation_id = get_correlation_id()
    try:
        REQUEST_COUNTER.inc()
        logger.info("Received /load-data request.", extra={"correlation_id": correlation_id})

        header_data = parse_x_params()
        dataset_name = header_data.get('dataset_name')
        format_type = header_data.get('format')

        if not dataset_name:
            ERROR_COUNTER.inc()
            return jsonify({"status": "error", "message": "Parameter 'dataset_name' is required"}), 400

        dataset_name = sanitize_dataset_name(dataset_name)

        if not format_type or format_type.lower() not in ['csv', 'xlsx', 'xls', 'json', 'parquet']:
            ERROR_COUNTER.inc()
            return jsonify({
                "status": "error",
                "message": "Parameter 'format' must be one of ['csv', 'xlsx', 'xls', 'json', 'parquet']"
            }), 400

        ipc_data = request.get_data()
        if not ipc_data:
            ERROR_COUNTER.inc()
            return jsonify({"status": "error", "message": "No data received"}), 400

        logger.info(f"Received {len(ipc_data)} bytes of Arrow IPC data.",
                    extra={"correlation_id": correlation_id})

        arrow_table = ipc_to_table(ipc_data)
        rows_in = arrow_table.num_rows
        cols_in = arrow_table.num_columns

        converted_data, actual_format = load_arrow_to_format(arrow_table, format_type)

        # Persist to shared volume (use actual_format e.g. xls→xlsx)
        file_path = save_output_file(converted_data, dataset_name, actual_format)

        SUCCESS_COUNTER.inc()
        logger.info(f"Successfully converted data to {format_type} format.",
                    extra={"correlation_id": correlation_id, "dataset_name": dataset_name})

        save_metadata("load-data", dataset_name, {
            "rows_in": rows_in, "cols_in": cols_in,
            "format": format_type, "output_file": file_path,
        }, start_time)

        return jsonify({
            "status": "success",
            "message": f"Data loaded successfully and saved to {file_path}"
        }), 200

    except ValueError as ve:
        ERROR_COUNTER.inc()
        logger.error(str(ve), extra={"correlation_id": correlation_id})
        return jsonify({"status": "error", "message": str(ve)}), 400
    except Exception as e:
        ERROR_COUNTER.inc()
        logger.exception("Error during /load-data processing.", extra={"correlation_id": correlation_id})
        return jsonify({"status": "error", "message": str(e)}), 500
