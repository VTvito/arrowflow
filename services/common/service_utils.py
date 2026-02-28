"""
Shared service utilities for all ETL microservices.

Eliminates boilerplate: Prometheus counters, /health & /metrics endpoints,
metadata writing, X-Params parsing, and correlation ID management.

Usage:
    from common.service_utils import (
        create_service_counters, register_standard_endpoints,
        parse_x_params, save_metadata, get_correlation_id,
    )
"""

import json
import os
import time
import uuid
from datetime import datetime, timezone

from common.health import create_health_response
from common.json_utils import NpEncoder
from common.path_utils import ensure_dataset_dirs
from flask import Response, g, jsonify, request
from prometheus_client import Counter, generate_latest

# ── Prometheus helpers ──────────────────────────────────────────────

def create_service_counters(service_slug):
    """
    Create the standard 3 Prometheus counters for a service.

    Args:
        service_slug: e.g. "clean_nan", "extract_csv" (underscores, no dashes)

    Returns:
        tuple of (REQUEST_COUNTER, SUCCESS_COUNTER, ERROR_COUNTER)
    """
    return (
        Counter(f'{service_slug}_requests_total', f'Total requests for {service_slug}'),
        Counter(f'{service_slug}_success_total', f'Successful requests for {service_slug}'),
        Counter(f'{service_slug}_error_total', f'Failed requests for {service_slug}'),
    )


# ── Standard endpoints ─────────────────────────────────────────────

def register_standard_endpoints(bp, service_name):
    """
    Register /health and /metrics endpoints on a Flask Blueprint.

    Args:
        bp: Flask Blueprint instance
        service_name: human-readable service name, e.g. "clean-nan-service"
    """

    @bp.route('/health', methods=['GET'])
    def health():
        return jsonify(create_health_response(service_name)), 200

    @bp.route('/metrics', methods=['GET'])
    def metrics():
        return Response(generate_latest(), mimetype="text/plain")


# ── Request helpers ─────────────────────────────────────────────────

def get_correlation_id():
    """
    Read or generate a correlation ID for the current request.

    Reads from X-Correlation-ID header; if absent, generates a UUID.
    Stores in Flask's ``g`` object for reuse within the request.
    """
    if not hasattr(g, '_correlation_id'):
        g._correlation_id = request.headers.get(
            'X-Correlation-ID', str(uuid.uuid4())
        )
    return g._correlation_id


def parse_x_params():
    """
    Parse the X-Params header as JSON.

    Raises ValueError with a 400-friendly message if the header contains
    malformed JSON (instead of silently defaulting to {}).
    """
    raw = request.headers.get('X-Params', '{}')
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Malformed JSON in X-Params header: {exc}") from exc


# ── Metadata helper ────────────────────────────────────────────────

def save_metadata(service_name, dataset_name, extra_fields=None, start_time=None):
    """
    Write a JSON metadata file for a completed service invocation.

    Args:
        service_name: e.g. "clean-nan"
        dataset_name: sanitized dataset name
        extra_fields: dict of service-specific stats to merge into metadata
        start_time: ``time.time()`` captured at request start (for duration_sec)

    Returns:
        str: path to the written metadata file
    """
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    _, metadata_dir = ensure_dataset_dirs(dataset_name)

    # Normalise service name for filename (e.g. "clean-nan" → "clean_nan")
    slug = service_name.replace("-", "_")
    metadata_path = os.path.join(metadata_dir, f"metadata_{slug}_{timestamp}.json")

    metadata = {
        "service_name": service_name,
        "dataset_name": dataset_name,
        "timestamp": timestamp,
        "correlation_id": get_correlation_id() if _has_request_context() else None,
    }
    if start_time is not None:
        metadata["duration_sec"] = round(time.time() - start_time, 3)
    if extra_fields:
        metadata.update(extra_fields)

    with open(metadata_path, "w") as f:
        json.dump(metadata, f, cls=NpEncoder, indent=2)

    return metadata_path


def _has_request_context():
    """Check whether we're inside a Flask request context."""
    try:
        _ = request.method  # noqa: F841
        return True
    except RuntimeError:
        return False
