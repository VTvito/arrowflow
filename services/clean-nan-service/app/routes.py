import logging
from flask import Blueprint, jsonify, request, Response
from app.clean import apply_transformations
from prometheus_client import Counter, generate_latest
from common.arrow_utils import ipc_to_table, table_to_ipc

bp = Blueprint('clean-nan', __name__)

# Logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger = logging.getLogger('clean-nan-service')

# Monitoring counters
REQUEST_COUNTER = Counter('clean_nan_requests_total', 'Total requests for the clean NaN service')
SUCCESS_COUNTER = Counter('clean_nan_success_total', 'Total successful requests for the clean NaN service')
ERROR_COUNTER = Counter('clean_nan_error_total', 'Total failed requests for the clean NaN service')

@bp.route('/clean-nan', methods=['POST'])
def clean_nan():
    """
    API Endpoint to clean data by removing NaN values.

    Request Body:
    - Arrow IPC data in binary format.

    Returns:
    - Cleaned Arrow IPC data in binary format.
    """
    try:
        REQUEST_COUNTER.inc()
        logger.info("Received /clean-nan request.")

        # Get the binary data from the request body
        ipc_data = request.get_data()
        if not ipc_data:
            logger.error("No data received in /clean-nan request.")
            ERROR_COUNTER.inc()
            return jsonify({"status": "error", "message": "No data received"}), 400

        logger.info(f"Received {len(ipc_data)} bytes of Arrow IPC data.")

        # Deserialize Arrow Table from IPC data
        arrow_table = ipc_to_table(ipc_data)

        # Apply transformations
        cleaned_arrow_table = apply_transformations(arrow_table)
        logger.info(f"Applied transformations, resulting in {cleaned_arrow_table.num_rows} rows.")

        # Serialize the cleaned Arrow Table to IPC format
        cleaned_ipc_data = table_to_ipc(cleaned_arrow_table)

        SUCCESS_COUNTER.inc()
        logger.info("Successfully cleaned and serialized data.")

        return Response(cleaned_ipc_data, mimetype="application/vnd.apache.arrow.stream"), 200

    except Exception as e:
        ERROR_COUNTER.inc()
        logger.exception("Error during /clean-nan processing.")
        return jsonify({"status": "error", "message": str(e)}), 500

@bp.route('/metrics', methods=['GET'])
def metrics():
    """
    Prometheus monitoring endpoint.

    Returns:
    - Metrics collected by Prometheus as plaintext.
    """
    return Response(generate_latest(), mimetype="text/plain")