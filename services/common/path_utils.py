import os
import re
from pathlib import Path

DATA_ROOT = os.getenv("ETL_DATA_ROOT", "/app/data")
_DATASET_NAME_PATTERN = re.compile(r"^[A-Za-z0-9._-]{1,128}$")


def sanitize_dataset_name(dataset_name):
    if not isinstance(dataset_name, str):
        raise ValueError("Parameter 'dataset_name' must be a string")

    cleaned = dataset_name.strip()
    if not cleaned:
        raise ValueError("Parameter 'dataset_name' cannot be empty")

    if not _DATASET_NAME_PATTERN.fullmatch(cleaned):
        raise ValueError("Parameter 'dataset_name' contains invalid characters")

    if cleaned in {".", ".."}:
        raise ValueError("Parameter 'dataset_name' is not allowed")

    return cleaned


def ensure_dataset_dirs(dataset_name):
    safe_name = sanitize_dataset_name(dataset_name)
    dataset_folder = os.path.join(DATA_ROOT, safe_name)

    resolved_dataset_folder = str(Path(dataset_folder).resolve())
    resolved_base = str(Path(DATA_ROOT).resolve())
    if os.path.commonpath([resolved_dataset_folder, resolved_base]) != resolved_base:
        raise ValueError("Parameter 'dataset_name' must stay under /app/data")

    dataset_folder = resolved_dataset_folder
    metadata_dir = os.path.join(dataset_folder, "metadata")
    os.makedirs(metadata_dir, exist_ok=True)
    return dataset_folder, metadata_dir


def resolve_input_path(file_path, must_exist=True):
    if not isinstance(file_path, str) or not file_path.strip():
        raise ValueError("Parameter 'file_path' must be a non-empty string")

    candidate = Path(file_path).expanduser()
    if not candidate.is_absolute():
        candidate = Path(DATA_ROOT) / candidate

    resolved_candidate = candidate.resolve()
    resolved_base = Path(DATA_ROOT).resolve()

    if os.path.commonpath([str(resolved_candidate), str(resolved_base)]) != str(resolved_base):
        raise ValueError("Parameter 'file_path' must stay under /app/data")

    if must_exist and not resolved_candidate.exists():
        raise FileNotFoundError(f"File not found: {resolved_candidate}")

    return str(resolved_candidate)
