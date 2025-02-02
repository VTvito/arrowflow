import logging
from flask import Blueprint, jsonify, request, Response
from app.load import load_arrow_to_format
from common.arrow_utils import ipc_to_table
from common.json_utils import NpEncoder
from prometheus_client import Counter, generate_latest
import os
import time
import json
from datetime import datetime

bp = Blueprint('load-data', __name__)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(name)s %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger('load-data-service')

REQUEST_COUNTER = Counter('load_data_requests_total', 'Total requests for the load data service')
SUCCESS_COUNTER = Counter('load_data_success_total', 'Total successful requests for the load data service')
ERROR_COUNTER = Counter('load_data_error_total', 'Total failed requests for the load data service')

@bp.route('/load-data', methods=['POST'])
def load_data():
    """
    API Endpoint to load cleaned data into a specified format.

    Query params:
      - format (str): desired output format ('csv','excel','json')
      - dataset_name (str, optional): name of dataset for metadata
    Body:
      - Arrow IPC in binary
    """
    start_time = time.time()
    try:
        REQUEST_COUNTER.inc()
        logger.info("Received /load-data request.")

        # get 'format' and 'dataset_name' from query
        format_type = request.args.get('format', default=None, type=str)
        dataset_name = request.args.get('dataset_name', 'no_dataset')

        if not format_type or format_type.lower() not in ['csv', 'excel', 'json']:
            logger.error("Missing or unsupported 'format' parameter.")
            ERROR_COUNTER.inc()
            return jsonify({
                "status": "error",
                "message": "Parameter 'format' must be one of ['csv','excel','json']"
            }), 400

        logger.info(f"Requested format for loading data: {format_type}")

        # get Arrow IPC
        ipc_data = request.get_data()
        if not ipc_data:
            logger.error("No data received in /load-data request.")
            ERROR_COUNTER.inc()
            return jsonify({"status": "error", "message": "No data received"}), 400

        logger.info(f"Received {len(ipc_data)} bytes of Arrow IPC data.")

        # Deserialize Arrow table
        arrow_table = ipc_to_table(ipc_data)
        rows_in = arrow_table.num_rows
        cols_in = arrow_table.num_columns

        # Convert to desired format
        from app.load import load_arrow_to_format
        converted_data = load_arrow_to_format(arrow_table, format_type)

        SUCCESS_COUNTER.inc()
        logger.info(f"Successfully converted data to {format_type} format.")

        # Save processed file
        dataset_folder = f"/app/data/{dataset_name}"
        os.makedirs(dataset_folder, exist_ok=True)
        processed_dir = os.path.join(dataset_folder, "processed")
        os.makedirs(processed_dir, exist_ok=True)

        timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        filename = f"step_load_{timestamp}.{format_type.lower()}"
        file_path = os.path.join(processed_dir, filename)

        with open(file_path, 'wb') as f:
            f.write(converted_data)
        logger.info(f"Saved data to {file_path}")

        # Metadata
        end_time = time.time()
        metadata_dir = os.path.join(dataset_folder, "metadata")
        os.makedirs(metadata_dir, exist_ok=True)
        metadata_file = os.path.join(metadata_dir, f"metadata_load_{timestamp}.json")

        metadata = {
            "service_name": "load-data",
            "dataset_name": dataset_name,
            "rows_in": rows_in,
            "cols_in": cols_in,
            "format": format_type,
            "output_file": file_path,
            "duration_sec": round(end_time - start_time, 3),
            "timestamp": timestamp
        }
        with open(metadata_file, 'w') as f:
            json.dump(metadata, f, cls=NpEncoder, indent=2)
        logger.info(f"Metadata saved to {metadata_file}")

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
    Prometheus monitoring endpoint
    """
    return Response(generate_latest(), mimetype="text/plain")