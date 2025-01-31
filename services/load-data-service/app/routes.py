import logging
from flask import Blueprint, jsonify, request, Response
from app.load import load_arrow_to_format
from common.arrow_utils import ipc_to_table
from prometheus_client import Counter, generate_latest
import pyarrow as pa
import io
import os

bp = Blueprint('load-data', __name__)

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(name)s %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('load-data-service')

# Monitoring counters
REQUEST_COUNTER = Counter('load_data_requests_total', 'Total requests for the load data service')
SUCCESS_COUNTER = Counter('load_data_success_total', 'Total successful requests for the load data service')
ERROR_COUNTER = Counter('load_data_error_total', 'Total failed requests for the load data service')

@bp.route('/load-data', methods=['POST'])
def load_data():
    """
    API Endpoint to load cleaned data into a specified format.

    Request Body:
    - Arrow IPC data in binary format.
    - format (str): Desired output format ('csv', 'excel', 'json').

    Returns:
    - Status message.
    """
    try:
        REQUEST_COUNTER.inc()
        logger.info("Received /load-data request.")

        # Catch the 'format' parameter from query string
        format_type = request.args.get('format', default=None, type=str)
        if not format_type or format_type.lower() not in ['csv', 'excel', 'json']:
            logger.error("Missing or unsupported 'format' parameter in request.")
            ERROR_COUNTER.inc()
            return jsonify({
                "status": "error",
                "message": "Parameter 'format' is required and must be one of ['csv', 'excel', 'json']"
            }), 400

        logger.info(f"Requested format for loading data: {format_type}")

        # Catch binary data from request
        ipc_data = request.get_data()
        if not ipc_data:
            logger.error("No data received in /load-data request.")
            ERROR_COUNTER.inc()
            return jsonify({"status": "error", "message": "No data received"}), 400

        logger.info(f"Received {len(ipc_data)} bytes of Arrow IPC data.")

        # Deserialize Arrow Table from IPC data
        arrow_table = ipc_to_table(ipc_data)

        # Convert Arrow table in desired format
        try:
            converted_data = load_arrow_to_format(arrow_table, format_type)
        except Exception as e:
            logger.error(f"Conversion to format {format_type} failed: {e}")
            ERROR_COUNTER.inc()
            return jsonify({
                "status": "error",
                "message": f"Conversion failed: {str(e)}"
            }), 500

        SUCCESS_COUNTER.inc()
        logger.info(f"Successfully converted data to {format_type} format.")

        # Define path file in shared volume
        output_dir = '/app/data/processed_data'  # Mounted directory in shared volume
        os.makedirs(output_dir, exist_ok=True)  # Create directory if not exists

        # Define the name of file based on format and current date
        from datetime import datetime
        timestamp = datetime.utcnow().strftime('%Y%m%d%H%M%S')
        filename = f"cleaned_data_{timestamp}.{format_type.lower()}"
        file_path = os.path.join(output_dir, filename)

        # Save the file
        with open(file_path, 'wb') as f:
            f.write(converted_data)
        logger.info(f"Saved cleaned data to {file_path}")

        # Return confirmation message
        return jsonify({
            "status": "success",
            "message": f"Data loaded successfully and saved to {file_path}"
        }), 200

    except Exception as e:
        ERROR_COUNTER.inc()
        logger.exception("Error during /load-data processing.")
        return jsonify({"status": "error", "message": str(e)}), 500

@bp.route('/metrics', methods=['GET'])
def metrics():
    """
    Prometheus monitoring endpoint.

    Returns:
    - Metrics catched from Prometheus in plain text.
    """
    return Response(generate_latest(), mimetype="text/plain")