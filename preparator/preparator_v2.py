import requests
import logging

class Preparator:
    def __init__(self, services_config):
        
        """
        services_config è un dizionario che mappa i nomi logici dei microservizi ai loro endpoint.
        Esempio:
        {
            "extract_csv": "http://extract-csv-service:5001/extract-csv",
            ...
        }
        """
        self.services = services_config
        self.logger = logging.getLogger("Preparator")
        self.session = requests.Session()

    def run_service_json_in_ipc_out(self, service_key, json_data):
        """
        Metodo di supporto per chiamare un servizio che accetta JSON in input
        e restituisce Arrow IPC in output.
        """
        self.logger.info(f"Calling {service_key} with JSON data: {json_data}")
        url = self.services[service_key]
        resp = self.session.post(url, json=json_data)
        resp.raise_for_status()
        # Dovrebbe restituire IPC in caso di successo
        return resp.content

    def run_service_ipc_in_ipc_out(self, service_key, ipc_data):
        """
        Metodo di supporto per chiamare un servizio che accetta Arrow IPC in input
        e restituisce Arrow IPC in output.
        """
        self.logger.info(f"Calling {service_key} with IPC data of size {len(ipc_data)} bytes")
        url = self.services[service_key]
        resp = self.session.post(url, data=ipc_data, headers={"Content-Type": "application/vnd.apache.arrow.stream"})
        resp.raise_for_status()
        return resp.content

    def extract_csv(self, client_id, file_path):
        # Chiama extract-csv-service con JSON
        return self.run_service_json_in_ipc_out("extract_csv", {"client_id": client_id, "file_path": file_path})

    def clean_nan(self, ipc_data):
        # Chiama clean-nan-service con IPC
        return self.run_service_ipc_in_ipc_out("clean_nan", ipc_data)

    def load_data(self, ipc_data, format='csv'):
        # Chiama load-data-service con IPC e parametro format nella query
        self.logger.info(f"Loading data with format: {format}")
        url = f"{self.services['load_data']}?format={format}"
        resp = self.session.post(url, data=ipc_data, headers={"Content-Type": "application/vnd.apache.arrow.stream"})
        resp.raise_for_status()
        # Se load restituisce IPC in caso di successo, lo ritorniamo
        return resp.content

    def extract_excel(self, client_id, file_path):
        return self.run_service_json_in_ipc_out("extract_excel", {
            "client_id": client_id,
            "file_path": file_path
        })

    def extract_api(self, client_id, api_url, api_params={}, auth_type=None, auth_value=None):
        json_data = {
            "client_id": client_id,
            "api_url": api_url,
            "api_params": api_params
        }
        if auth_type is not None:
            json_data["auth_type"] = auth_type
        if auth_value is not None:
            json_data["auth_value"] = auth_value

        return self.run_service_json_in_ipc_out("extract_api", json_data)

    def extract_sql(self, client_id, db_url, query):
        json_data = {
            "client_id": client_id,
            "db_url": db_url,
            "query": query
        }
        return self.run_service_json_in_ipc_out("extract_sql", json_data)
    
    def delete_columns(self, ipc_data, columns):
        """
        Chiama il microservizio delete-columns con Arrow IPC e query param columns
        """
        service_key = "delete_columns" 
        url = self.services[service_key]
        # Costruiamo la query string
        import urllib.parse
        qs = urllib.parse.urlencode({
            "columns": ",".join(columns)
        })
        full_url = f"{url}?{qs}"

        self.logger.info(f"Calling {service_key} with columns {columns}")
        resp = self.session.post(full_url, data=ipc_data, headers={"Content-Type": "application/vnd.apache.arrow.stream"})
        resp.raise_for_status()
        return resp.content