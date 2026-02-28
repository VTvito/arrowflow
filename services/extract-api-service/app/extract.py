import ipaddress
import logging
import os
import socket
from urllib.parse import urlparse

import json

import pandas as pd
import pyarrow as pa
import requests

logger = logging.getLogger('extract-api-service')


def _is_private_ip(ip_str):
    ip_obj = ipaddress.ip_address(ip_str)
    return (
        ip_obj.is_private
        or ip_obj.is_loopback
        or ip_obj.is_link_local
        or ip_obj.is_multicast
        or ip_obj.is_reserved
        or ip_obj.is_unspecified
    )


def _validate_api_url(api_url):
    parsed = urlparse(api_url)
    if parsed.scheme not in {"http", "https"}:
        raise ValueError("Only http/https API URLs are allowed")

    if not parsed.hostname:
        raise ValueError("Invalid API URL: missing hostname")

    allow_private_urls = os.getenv("ALLOW_PRIVATE_API_URLS", "false").lower() in {"1", "true", "yes"}
    if allow_private_urls:
        return api_url

    try:
        resolved = socket.getaddrinfo(parsed.hostname, None)
    except socket.gaierror as exc:
        raise ValueError(f"Unable to resolve API host: {parsed.hostname}") from exc

    for entry in resolved:
        ip_str = entry[4][0]
        if _is_private_ip(ip_str):
            raise ValueError("API URL points to a private/local network address, request blocked")

    return api_url

def extract_from_api(api_url, api_params, auth_type=None, auth_value=None):
    """
    Execute endpoint API call and return the data like DataFrame.
    Auth is supported by API key
    """
    try:
        safe_api_url = _validate_api_url(api_url)
        headers = {}

        # Configure header of auth (optional)
        if auth_type is not None:
            if auth_value is None:
                raise ValueError(f"auth_value is required when auth_type='{auth_type}'")
            if auth_type == "api_key":
                headers["x-api-key"] = auth_value
            else:
                raise ValueError(f"Type of auth '{auth_type}' not supported")

        # GET request
        response = requests.get(safe_api_url, params=api_params, headers=headers, timeout=30)
        response.raise_for_status()

        # Convert the response in DataFrame
        try:
            body = response.json()
        except json.JSONDecodeError as e:
            raise ValueError(
                f"API returned non-JSON response (status {response.status_code})"
            ) from e

        df = pd.json_normalize(body)

        # Convert pandas DataFrame to Arrow Table
        arrow_table = pa.Table.from_pandas(df)
        logger.info(
            f"Converted DataFrame to Arrow Table with "
            f"{arrow_table.num_rows} rows, {arrow_table.num_columns} columns."
        )
        return arrow_table

    except requests.RequestException as e:
        raise ValueError(f"API Error: {str(e)}") from e
