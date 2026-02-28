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

        # Read CSV with pandas
        df = pd.read_csv(safe_file_path)
        logger.info(f"Loaded CSV file {safe_file_path} into DataFrame with shape {df.shape}")

        # Convert pandas DataFrame to Arrow Table
        arrow_table = pa.Table.from_pandas(df)
        logger.info(
            f"Converted DataFrame to Arrow Table with "
            f"{arrow_table.num_rows} rows, {arrow_table.num_columns} columns."
        )

        return arrow_table
    except Exception as e:
        logger.error(f"Failed to load CSV into Arrow format: {e}")
        raise
