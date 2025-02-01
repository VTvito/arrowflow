from flask import Blueprint, jsonify, request
from app.join import join_datasets
from common.arrow_utils import table_to_ipc, ipc_to_table
import os
import pandas as pd
from prometheus_client import Counter, generate_latest
from flask import Response


bp = Blueprint('join-datasets', __name__)

DATA_FOLDER = '/app/data'

@bp.route('/join-datasets', methods=['POST'])
def join_datasets_endpoint():
    try:
        # Parametri dal body della richiesta
        client_id = request.json.get('client_id', 'client_id')
        dataset_name = request.json.get('dataset_name', 'dataset_name')
        file_paths = request.json.get('file_paths')  # Lista di file CSV
        join_key = request.json.get('join_key')  # Chiave di join
        join_type = request.json.get('join_type', 'inner')  # Tipo di join
        
        if not file_paths or len(file_paths) < 2:
            return jsonify({"error": "Devono essere forniti almeno due file per il join"}), 400
        
        if not join_key:
            return jsonify({"error": "La chiave di join è richiesta"}), 400

        # Esegui il join
        joined_data = join_datasets(file_paths, join_key, join_type)

        # Crea la cartella del cliente se non esiste e salva il dataset joinato 
        client_folder = os.path.join(DATA_FOLDER, client_id, 'join-datasets')
        os.makedirs(client_folder, exist_ok=True)
        file_path = os.path.join(client_folder, f'{dataset_name}.csv')
        joined_data.to_csv(file_path, index=False)

        return jsonify({
            "status": "success",
            "data_preview": joined_data.head().to_dict(),
            "file_path": file_path
        }), 200

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@bp.route('/get-joined-dataset/<client_id>/<dataset_name>', methods=['GET'])
def get_joined_dataset(client_id, dataset_name):
    try:
        # Percorso al dataset combinato
        file_path = os.path.join(DATA_FOLDER, client_id, 'join-datasets', f'{dataset_name}.csv')
        if not os.path.exists(file_path):
            return jsonify({"error": "Dataset combinato non trovato"}), 404

        data = pd.read_csv(file_path)
        return jsonify(data.to_dict()), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500
    

# Endpoint per il monitoraggio

REQUEST_COUNTER = Counter('service_requests_total', 'Total number of requests for this service')

@bp.route('/metrics', methods=['GET'])
def metrics():
    REQUEST_COUNTER.inc()  # Incrementa ogni volta che viene richiesta la metrica
    return Response(generate_latest(), mimetype="text/plain")