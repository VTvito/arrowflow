import pyarrow as pa
import logging
import pandas as pd

logger = logging.getLogger('clean-nan-service')

def apply_transformations(arrow_table):
    """
    Cleans the data by dropping rows with null values.

    Args:
        arrow_table (pyarrow.Table): Input Arrow Table to be cleaned.

    Returns:
        pyarrow.Table: Cleaned Arrow Table with no null values.
    """
    try:
        # Convert to Pandas for easier manipulation
        df = arrow_table.to_pandas()
        logger.info(f"Converted Arrow Table to Pandas DataFrame with {df.shape[0]} rows and {df.shape[1]} columns.")

        # Drop rows with NaN values
        df_cleaned = df.dropna()
        logger.info(f"Dropped NaN values, resulting in {df_cleaned.shape[0]} rows.")

        # Convert back to Arrow Table
        cleaned_arrow_table = pa.Table.from_pandas(df_cleaned)
        logger.info(f"Converted cleaned Pandas DataFrame back to Arrow Table with {cleaned_arrow_table.num_rows} rows.")

        return cleaned_arrow_table
    except Exception as e:
        logger.error(f"Failed to clean data: {e}")
        raise