from flask import Blueprint, jsonify, request
from app.columns import get_columns_from_csv
import os
from prometheus_client import Counter, generate_latest
from common.arrow_utils import table_to_ipc, ipc_to_table
from flask import Response

bp = Blueprint('get-columns-csv', __name__)

DATA_FOLDER = '/app/data'

@bp.route('/get-columns-csv', methods=['POST'])
def get_columns():
    try:
        # Parametri dinamici dal body della richiesta
        dataset_name = request.json.get('dataset', 'dataset_name')
        client_id = request.json.get('client_id', 'client_id')
        file_path = request.json.get('file_path')

        if not file_path:
            return jsonify({"error": "Il parametro file_path è richiesto"}), 400

        # Percorso per la cartella del cliente
        client_folder = os.path.join(DATA_FOLDER, client_id, 'get-columns-csv')
        os.makedirs(client_folder, exist_ok=True)

        # Ottieni le colonne del CSV
        columns = get_columns_from_csv(file_path)

        # Salva la lista delle colonne in un file
        columns_file_path = os.path.join(client_folder, f'{dataset_name}.csv')
        with open(columns_file_path, 'w') as f:
            for col in columns:
                f.write(f"{col}\n")

        return jsonify({
            "status": "success",
            "columns": columns,
            "file_path": columns_file_path
        }), 200

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@bp.route('/get-columns-csv/<client_id>/<dataset_name>', methods=['GET'])
def get_columns_file(client_id, dataset_name):
    try:
        # Percorso per la cartella specifica del cliente
        columns_file_path = os.path.join(DATA_FOLDER, client_id, 'get-columns-csv' f'{dataset_name}.csv')
        
        if not os.path.exists(columns_file_path):
            return jsonify({"status": "error", "message": "File delle colonne non disponibile"}), 400

        with open(columns_file_path, 'r') as f:
            columns = [line.strip() for line in f.readlines()]

        return jsonify({"columns": columns}), 200

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500
    

# Endpoint per il monitoraggio

REQUEST_COUNTER = Counter('service_requests_total', 'Total number of requests for this service')

@bp.route('/metrics', methods=['GET'])
def metrics():
    REQUEST_COUNTER.inc()  # Incrementa ogni volta che viene richiesta la metrica
    return Response(generate_latest(), mimetype="text/plain")