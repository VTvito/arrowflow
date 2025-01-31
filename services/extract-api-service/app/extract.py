import pandas as pd
import requests
import logging

logger = logging.getLogger('extract-api-service')

def extract_from_api(api_url, api_params, auth_type=None, auth_value=None):
    """
    Execute endpoint API call and return the data like DataFrame.
    Auth is supported by API key
    """
    try:
        headers = {}

        # Configure header of auth
        if auth_type == "api_key":
            headers["x-api-key"] = auth_value
        else:
            raise ValueError(f"Type of auth '{auth_type}' not supported")

        # GET request
        response = requests.get(api_url, params=api_params, headers=headers, timeout=30)
        response.raise_for_status()

        # Convert the response in DataFrame
        data = pd.json_normalize(response.json())
        return data

    except requests.RequestException as e:
        raise ValueError(f"API Error: {str(e)}")
