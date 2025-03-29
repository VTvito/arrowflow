import pyarrow as pa
import pandas as pd
import logging

logger = logging.getLogger('delete-columns-service')

def drop_columns_arrow(arrow_table, columns_to_delete):
    """
    Remove specified columns from an Arrow Table, returning (new_table, removed_count).
    """
    try:
        df = arrow_table.to_pandas()
        cols_before = df.shape[1]
        existing_cols = set(df.columns)
        to_remove_actual = [c for c in columns_to_delete if c in existing_cols]

        df.drop(columns=columns_to_delete, inplace=True, errors='ignore')
        removed_count = len(to_remove_actual)
        
        new_table = pa.Table.from_pandas(df)
        return new_table, removed_count
    except Exception as e:
        logger.error(f"Failed to drop columns {columns_to_delete}: {e}")
        raise
