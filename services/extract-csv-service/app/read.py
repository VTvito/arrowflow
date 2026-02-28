import logging

import pandas as pd
import pyarrow as pa
from common.path_utils import resolve_input_path

logger = logging.getLogger('extract-csv-service')

def load_csv_to_arrow(file_path):
    """
    Reads a CSV file using Pandas and converts it into an Apache Arrow Table.

    Args:
        file_path (str): The path to the CSV file.

    Returns:
        pyarrow.Table: Table containing the CSV data in Arrow format.
    """
    try:
        safe_file_path = resolve_input_path(file_path)
        df = pd.read_csv(safe_file_path)
        return pa.Table.from_pandas(df)
    except Exception as e:
        logger.error(f"Failed to load CSV into Arrow format: {e}")
        raise
