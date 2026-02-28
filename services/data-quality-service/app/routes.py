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
from flask import Blueprint, Response, jsonify, request

from app.dq import basic_quality_checks

bp = Blueprint('data_quality', __name__)
logger = logging.getLogger('data-quality-service')

REQUEST_COUNTER, SUCCESS_COUNTER, ERROR_COUNTER = create_service_counters('data_quality')
register_standard_endpoints(bp, 'data-quality-service')


def _get_failing_checks(checks: dict) -> dict:
    """
    Return a dict of only the checks that have pass=False.
    Handles both boolean results (e.g. min_rows) and dict results
    (e.g. null_ratio, duplicates) produced by basic_quality_checks().
    """
    failing = {}
    for key, value in checks.items():
        if isinstance(value, bool) and not value:
            failing[key] = value
        elif isinstance(value, dict):
            if "pass" in value and not value["pass"]:
                failing[key] = value
            else:
                # Nested dicts (e.g. check_column_types, check_value_range)
                nested_failures = {
                    col: v for col, v in value.items()
                    if isinstance(v, dict) and v.get("pass") is False
                }
                if nested_failures:
                    failing[key] = nested_failures
    return failing


@bp.route('/data-quality', methods=['POST'])
def data_quality():
    """Perform quality checks on Arrow IPC data and return data unchanged."""
    start_time = time.time()
    correlation_id = get_correlation_id()
    try:
        REQUEST_COUNTER.inc()
        logger.info("Received /data-quality request.", extra={"correlation_id": correlation_id})

        header_data = parse_x_params()
        dataset_name = header_data.get('dataset_name')
        rules = header_data.get('rules')
        fail_on_errors = header_data.get('fail_on_errors', False)

        if not dataset_name:
            ERROR_COUNTER.inc()
            return jsonify({"status": "error", "message": "No dataset_name provided in header"}), 400

        dataset_name = sanitize_dataset_name(dataset_name)

        if not rules:
            logger.info("No rules provided in header. Using default rules.",
                        extra={"correlation_id": correlation_id})

        ipc_data = request.get_data()
        if not ipc_data:
            ERROR_COUNTER.inc()
            return jsonify({"status": "error", "message": "No Arrow IPC data received"}), 400

        arrow_table = ipc_to_table(ipc_data)
        rows_in = arrow_table.num_rows
        cols_in = arrow_table.num_columns

        dq_result = basic_quality_checks(arrow_table, rules)
        logger.info(
            f"Data quality checks completed for dataset '{dataset_name}' "
            f"with {rows_in} rows and {cols_in} columns.",
            extra={"correlation_id": correlation_id, "dataset_name": dataset_name},
        )

        # Quality gate: optionally abort the pipeline if any check fails
        if fail_on_errors:
            failing = _get_failing_checks(dq_result["checks"])
            if failing:
                ERROR_COUNTER.inc()
                logger.warning(
                    f"Quality gate triggered for dataset '{dataset_name}': {list(failing.keys())}",
                    extra={"correlation_id": correlation_id, "dataset_name": dataset_name},
                )
                return jsonify({
                    "status": "quality_failed",
                    "message": (
                        f"Data quality checks failed for dataset '{dataset_name}': "
                        f"{', '.join(failing.keys())}"
                    ),
                    "checks": failing,
                }), 422

        save_metadata("data-quality", dataset_name, {
            "rows_in": rows_in, "cols_in": cols_in,
            "dq_checks": dq_result["checks"],
        }, start_time)

        SUCCESS_COUNTER.inc()
        resp = Response(ipc_data, status=200, mimetype="application/vnd.apache.arrow.stream")
        resp.headers["X-Correlation-ID"] = correlation_id
        return resp

    except ValueError as ve:
        ERROR_COUNTER.inc()
        logger.error(str(ve), extra={"correlation_id": correlation_id})
        return jsonify({"status": "error", "message": str(ve)}), 400
    except Exception as e:
        ERROR_COUNTER.inc()
        logger.exception("Error in data quality service.", extra={"correlation_id": correlation_id})
        return jsonify({"status": "error", "message": str(e)}), 500
