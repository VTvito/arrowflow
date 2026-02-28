"""
Enriched health check for all ETL microservices.

Provides service identity, uptime, shared volume status, and disk space.

Usage:
    from common.health import create_health_response
    # In your /health endpoint:
    return jsonify(create_health_response("clean-nan-service")), 200
"""

import os
import shutil
import time
from datetime import datetime, timezone

_start_time = time.monotonic()


def create_health_response(service_name):
    """
    Build an enriched health check response.

    Returns a dict with:
      - status: "ok"
      - service: service name
      - version: from SERVICE_VERSION env var (default "1.0.0")
      - uptime_seconds: time since module was loaded
      - timestamp: current UTC ISO timestamp
      - checks.shared_volume: whether /app/data (ETL_DATA_ROOT) is accessible
      - checks.disk_space_mb: free disk space in MB on the data volume
    """
    data_root = os.getenv("ETL_DATA_ROOT", "/app/data")
    volume_ok = os.path.isdir(data_root)
    disk_free_mb = None
    if volume_ok:
        try:
            usage = shutil.disk_usage(data_root)
            disk_free_mb = round(usage.free / (1024 * 1024), 1)
        except OSError:
            disk_free_mb = None

    return {
        "status": "ok",
        "service": service_name,
        "version": os.getenv("SERVICE_VERSION", "1.0.0"),
        "uptime_seconds": round(time.monotonic() - _start_time, 1),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "checks": {
            "shared_volume": volume_ok,
            "disk_space_mb": disk_free_mb,
        },
    }
