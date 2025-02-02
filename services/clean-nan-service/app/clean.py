import pyarrow as pa
import pandas as pd
import logging

logger = logging.getLogger('clean-nan-service')

def apply_transformations(arrow_table):
    """
    Cleans the data by dropping rows with null values.
    Returns (cleaned_arrow_table, num_nan_removed).
    """
    try:
        df = arrow_table.to_pandas()
        logger.info(f"Converted to Pandas with shape {df.shape}")

        before_rows = df.shape[0]
        # Count total NaNs before drop
        total_null_before = df.isna().sum().sum()

        df_cleaned = df.dropna()

        after_rows = df_cleaned.shape[0]
        total_null_after = df_cleaned.isna().sum().sum()

        removed_null_count = total_null_before - total_null_after
        logger.info(f"Removed {removed_null_count} null cells. Rows from {before_rows} to {after_rows}")

        cleaned_arrow_table = pa.Table.from_pandas(df_cleaned)
        return (cleaned_arrow_table, removed_null_count)
    except Exception as e:
        logger.error(f"Failed to clean data: {e}")
        raise