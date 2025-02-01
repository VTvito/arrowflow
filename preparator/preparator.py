import requests
import logging

class Preparator:
    def __init__(self, services_config):
        """
        services_config è un dict che mappa i nomi logici dei microservizi ai loro endpoint.
        Esempio:
        {
            "extract": "http://extract-csv-service:5001/extract-csv",
            ...
        }
        """
        self.services = services_config
        self.logger = logging.getLogger("Preparator")
        self.session = requests.Session()

    def extract(self, client_id, file_path):
        self.logger.info(f"Extracting data from {file_path} for client {client_id}")
        extract_data = {"client_id": client_id, "file_path": file_path}
        resp = self.session.post(self.services['extract'], json=extract_data)
        resp.raise_for_status()
        ipc_data = resp.content
        self.logger.info(f"Extracted {len(ipc_data)} bytes from {file_path}")
        return ipc_data

    def clean(self, ipc_data):
        self.logger.info(f"Cleaning data (size: {len(ipc_data)} bytes)")
        resp = self.session.post(
            self.services['clean'],
            data=ipc_data,
            headers={"Content-Type": "application/vnd.apache.arrow.stream"}
        )
        resp.raise_for_status()
        cleaned_data = resp.content
        self.logger.info(f"Cleaned data size: {len(cleaned_data)} bytes")
        return cleaned_data

    def load(self, ipc_data, format='csv'):
        self.logger.info(f"Loading data with format: {format}")
        resp = self.session.post(
            f"{self.services['load']}?format={format}",
            data=ipc_data,
            headers={"Content-Type": "application/vnd.apache.arrow.stream"}
        )
        resp.raise_for_status()
        # load-data-service restituisce JSON con "status" e "message"
        result = resp.json()
        self.logger.info(f"Loaded data: {result.get('message')}")
        return result.get('message')