import logging
from flask import Blueprint, jsonify, request, Response
from app.columns import drop_columns_arrow
from common.arrow_utils import ipc_to_table, table_to_ipc
from prometheus_client import Counter, generate_latest

bp = Blueprint('delete-columns', __name__)

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger = logging.getLogger('delete-columns-service')

# Monitoring counters
REQUEST_COUNTER = Counter('delete_columns_requests_total', 'Total requests for the delete columns service')
SUCCESS_COUNTER = Counter('delete_columns_success_total', 'Total successful requests for the delete columns service')
ERROR_COUNTER = Counter('delete_columns_error_total', 'Total failed requests for the delete columns service')

@bp.route('/delete-columns', methods=['POST'])
def delete_columns():
    """
    API Endpoint that receives Arrow IPC + query params specifying columns to drop,
    returns Arrow IPC with those columns removed.
    Query params:
      - columns: comma-separated list of columns
    Body:
      - Arrow IPC bytes
    """
    try:
        REQUEST_COUNTER.inc()
        logger.info("Received /delete-columns request.")

        # Read the parameters from query string
        columns_str = request.args.get('columns', '')  # ex. "Col1,Col2"


        columns_to_delete = [col.strip() for col in columns_str.split(',') if col.strip()]
        if not columns_to_delete:
            logger.error("No columns specified via 'columns' query param.")
            ERROR_COUNTER.inc()
            return jsonify({"status": "error", "message": "No columns specified"}), 400

        # Read the Arrow bytes
        ipc_data = request.get_data()
        if not ipc_data:
            logger.error("No data (Arrow IPC) received.")
            ERROR_COUNTER.inc()
            return jsonify({"status": "error", "message": "No Arrow IPC data received"}), 400

        logger.info(f"Received {len(ipc_data)} bytes of Arrow IPC data.")
        
        # Deserialize Arrow Table from IPC data
        arrow_table = ipc_to_table(ipc_data)
        
        # Drop the columns
        updated_table = drop_columns_arrow(arrow_table, columns_to_delete)
        logger.info(f"Dropped columns {columns_to_delete}, new table has {updated_table.num_columns} columns.")

        # Serialize the Arrow Table to IPC format
        out_ipc = table_to_ipc(updated_table)

        SUCCESS_COUNTER.inc()
        logger.info("Successfully deleted columns and serialized result in Arrow IPC.")
        return Response(out_ipc, mimetype="application/vnd.apache.arrow.stream"), 200

    except Exception as e:
        ERROR_COUNTER.inc()
        logger.exception("Error during /delete-columns processing.")
        return jsonify({"status": "error", "message": str(e)}), 500

# Endpoint for monitoring
@bp.route('/metrics', methods=['GET'])
def metrics():
    return Response(generate_latest(), mimetype="text/plain")