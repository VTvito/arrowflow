"""
XCom File Utilities — Shared filesystem-based data transfer for Airflow tasks.

Instead of storing large Arrow IPC binary data in PostgreSQL via XCom,
this module saves IPC files to the shared Docker volume (/app/data/)
and passes only the file path through XCom.

This eliminates the XCom bottleneck for datasets >50k rows.
"""

import logging
import os
import uuid
from datetime import datetime, timezone

logger = logging.getLogger("xcom_file_utils")

SHARED_DATA_ROOT = "/app/data"


def save_ipc_to_shared(ipc_data: bytes, dataset_name: str, step_name: str) -> str:
    """
    Save Arrow IPC data to the shared volume.

    Args:
        ipc_data: Arrow IPC binary data.
        dataset_name: Name of the dataset (used for folder organization).
        step_name: Name of the pipeline step (e.g., 'extract_csv', 'clean_nan').

    Returns:
        The file path where data was saved (to be passed via XCom).
    """
    xcom_dir = os.path.join(SHARED_DATA_ROOT, dataset_name, "xcom")
    os.makedirs(xcom_dir, exist_ok=True)
    # Ensure the dataset dir and xcom dir are writable by all containers
    try:
        os.chmod(os.path.join(SHARED_DATA_ROOT, dataset_name), 0o777)
        os.chmod(xcom_dir, 0o777)
    except OSError:
        pass

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    unique_id = uuid.uuid4().hex[:8]
    filename = f"{step_name}_{timestamp}_{unique_id}.arrow"
    file_path = os.path.join(xcom_dir, filename)

    with open(file_path, "wb") as f:
        f.write(ipc_data)

    size_mb = len(ipc_data) / (1024 * 1024)
    logger.info(f"Saved IPC data to {file_path} ({size_mb:.2f} MB)")
    return file_path


def load_ipc_from_shared(file_path: str) -> bytes:
    """
    Load Arrow IPC data from the shared volume.

    Args:
        file_path: Path to the Arrow IPC file (returned by save_ipc_to_shared).

    Returns:
        Arrow IPC binary data.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"XCom IPC file not found: {file_path}")

    with open(file_path, "rb") as f:
        ipc_data = f.read()

    size_mb = len(ipc_data) / (1024 * 1024)
    logger.info(f"Loaded IPC data from {file_path} ({size_mb:.2f} MB)")
    return ipc_data


def cleanup_xcom_files(dataset_name: str) -> int:
    """
    Remove all XCom temporary files for a dataset after pipeline completion.

    Args:
        dataset_name: Name of the dataset.

    Returns:
        Number of files removed.
    """
    xcom_dir = os.path.join(SHARED_DATA_ROOT, dataset_name, "xcom")
    if not os.path.exists(xcom_dir):
        return 0

    removed = 0
    for filename in os.listdir(xcom_dir):
        file_path = os.path.join(xcom_dir, filename)
        if os.path.isfile(file_path):
            os.remove(file_path)
            removed += 1

    logger.info(f"Cleaned up {removed} XCom files for dataset '{dataset_name}'")
    return removed
