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

from app.extract import extract_from_sql, redact_db_url, validate_sql_query

bp = Blueprint('extract-sql', __name__)
logger = logging.getLogger('extract-sql-service')

REQUEST_COUNTER, SUCCESS_COUNTER, ERROR_COUNTER = create_service_counters('extract_sql')
register_standard_endpoints(bp, 'extract-sql-service')


@bp.route('/extract-sql', methods=['POST'])
def extract_data():
    """Extract data from a SQL database and return Arrow IPC."""
    start_time = time.time()
    correlation_id = get_correlation_id()
    try:
        REQUEST_COUNTER.inc()
        logger.info("Received /extract-sql request.", extra={"correlation_id": correlation_id})

        data = request.get_json()
        dataset_name = data.get('dataset_name')
        query = data.get('query')
        db_url = data.get('db_url')

        if not db_url or query is None:
            ERROR_COUNTER.inc()
            return jsonify({"status": "error", "message": "Parameters 'db_url' and 'query' are required"}), 400

        if not dataset_name:
            ERROR_COUNTER.inc()
            return jsonify({"status": "error", "message": "Parameter 'dataset_name' is required"}), 400

        dataset_name = sanitize_dataset_name(dataset_name)
        query = validate_sql_query(query)
        db_url_redacted = redact_db_url(db_url)

        logger.info(f"Extracting from SQL source for dataset '{dataset_name}'",
                    extra={"correlation_id": correlation_id})

        arrow_table = extract_from_sql(db_url, query)
        rows = arrow_table.num_rows
        cols = arrow_table.num_columns

        if rows == 0:
            logger.warning("Extracted Arrow table has no rows.", extra={"correlation_id": correlation_id})

        ipc_data = table_to_ipc(arrow_table)
        SUCCESS_COUNTER.inc()
        logger.info(
            f"Extracted SQL data with {rows} rows, {cols} cols for dataset '{dataset_name}'.",
            extra={"correlation_id": correlation_id, "dataset_name": dataset_name},
        )

        save_metadata("extract-sql", dataset_name, {
            "db_url_redacted": db_url_redacted,
            "query_preview": query[:200],
            "rows_out": rows, "cols_out": cols,
        }, start_time)

        resp = Response(ipc_data, status=200, mimetype="application/vnd.apache.arrow.stream")
        resp.headers["X-Correlation-ID"] = correlation_id
        return resp

    except ConnectionError as ce:
        ERROR_COUNTER.inc()
        logger.error(str(ce), extra={"correlation_id": correlation_id})
        return jsonify({"status": "error", "message": str(ce)}), 400
    except ValueError as ve:
        ERROR_COUNTER.inc()
        logger.error(str(ve), extra={"correlation_id": correlation_id})
        return jsonify({"status": "error", "message": str(ve)}), 400
    except Exception as e:
        ERROR_COUNTER.inc()
        logger.exception("Error during /extract-sql processing.", extra={"correlation_id": correlation_id})
        return jsonify({"status": "error", "message": str(e)}), 500
