import io
import logging
import os
from datetime import datetime, timezone

import pandas as pd
import pyarrow.parquet as pq
from common.path_utils import ensure_dataset_dirs

logger = logging.getLogger('load-data-service')

def load_arrow_to_format(arrow_table, format_type):
    """
    Converts an Arrow Table to the specified format ('csv','excel','json','parquet').
    Returns bytes of the converted data.
    """
    try:
        if not isinstance(format_type, str) or not format_type.strip():
            raise ValueError("Unsupported format: format_type must be a non-empty string")

        normalized_format = format_type.lower().strip()

        # Parquet can be written directly from Arrow without Pandas conversion
        if normalized_format == 'parquet':
            output = io.BytesIO()
            pq.write_table(arrow_table, output, compression='snappy')
            logger.info("Converted Arrow Table to Parquet format (snappy compression).")
            return output.getvalue(), normalized_format

        df = arrow_table.to_pandas()
        logger.info(f"Converted Arrow Table to Pandas DataFrame with {df.shape[0]} rows and {df.shape[1]} columns.")

        if normalized_format == 'csv':
            output = df.to_csv(index=False)
            logger.info("Converted DataFrame to CSV format.")
            return output.encode('utf-8'), normalized_format

        elif normalized_format in ('xlsx', 'xls'):
            # xlsxwriter always produces .xlsx; normalise so callers
            # (including save_output_file) use the correct extension.
            if normalized_format == 'xls':
                logger.info("Normalising format 'xls' → 'xlsx' (xlsxwriter output).")
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                df.to_excel(writer, index=False, sheet_name='Sheet1')
            logger.info("Converted DataFrame to Excel format.")
            return output.getvalue(), 'xlsx'

        elif normalized_format == 'json':
            output = df.to_json(orient='records')
            logger.info("Converted DataFrame to JSON format.")
            return output.encode('utf-8'), normalized_format

        else:
            logger.error(f"Unsupported format: {format_type}")
            raise ValueError(f"Unsupported format: {format_type}")
    except Exception as e:
        logger.error(f"Failed to convert data: {e}")
        raise


def save_output_file(data: bytes, dataset_name: str, format_type: str) -> str:
    """
    Persist converted data to the shared volume under the dataset's processed/ folder.

    Args:
        data: Converted bytes to write (CSV, JSON, XLSX, Parquet).
        dataset_name: Sanitized dataset identifier.
        format_type: File extension / format name.

    Returns:
        Absolute path of the written file.
    """
    dataset_folder, _ = ensure_dataset_dirs(dataset_name)
    processed_dir = os.path.join(dataset_folder, "processed")
    os.makedirs(processed_dir, exist_ok=True)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    filename = f"step_load_{timestamp}.{format_type.lower()}"
    file_path = os.path.join(processed_dir, filename)

    with open(file_path, 'wb') as f:
        f.write(data)

    logger.info(f"Saved output file: {file_path}")
    return file_path
