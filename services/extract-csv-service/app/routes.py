import logging
from flask import Blueprint, jsonify, request, Response
from app.read import load_csv_to_arrow
from prometheus_client import Counter, generate_latest
from common.arrow_utils import table_to_ipc

bp = Blueprint('extract-csv', __name__)

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(name)s %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('extract-csv-service')

# Monitoring counters
REQUEST_COUNTER = Counter('extract_csv_requests_total', 'Total requests for the extract CSV service')
SUCCESS_COUNTER = Counter('extract_csv_success_total', 'Total successful requests for the extract CSV service')
ERROR_COUNTER = Counter('extract_csv_error_total', 'Total failed requests for the extract CSV service')

@bp.route('/extract-csv', methods=['POST'])
def extract_csv():
    """
    API Endpoint to extract data from a CSV file and serialize it into Apache Arrow IPC format.
    """
    try:
        REQUEST_COUNTER.inc()
        logger.info("Received /extract-csv request.")

        data = request.get_json()
        file_path = data.get('file_path')
        client_id = data.get('client_id')

        if not file_path:
            logger.error("Missing 'file_path' in request.")
            ERROR_COUNTER.inc()
            return jsonify({"status": "error", "message": "Parameter 'file_path' is required"}), 400

        logger.info(f"Client {client_id} requesting extraction from {file_path}.")

        # Load CSV into Arrow Table
        arrow_table = load_csv_to_arrow(file_path)

        # Base validation: checks that the table is not empty
        if arrow_table.num_rows == 0:
            logger.warning("Extracted Arrow table has no rows.")

        # Serialize Arrow Table to IPC format
        ipc_data = table_to_ipc(arrow_table)

        SUCCESS_COUNTER.inc()
        logger.info(f"Successfully extracted and serialized data for client {client_id}.")

        # Return binary data
        return Response(ipc_data, mimetype="application/vnd.apache.arrow.stream"), 200

    except Exception as e:
        ERROR_COUNTER.inc()
        logger.exception("Error during /extract-csv processing.")
        return jsonify({"status": "error", "message": str(e)}), 500

@bp.route('/metrics', methods=['GET'])
def metrics():
    """
    Prometheus monitoring endpoint.
    """
    return Response(generate_latest(), mimetype="text/plain")