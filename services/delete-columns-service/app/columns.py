import pyarrow as pa
import pandas as pd
import logging

logger = logging.getLogger('delete-columns-service')


def drop_columns_arrow(arrow_table, columns_to_delete):
    """
    Remove specified columns from Arrow Table and give back a new Arrow Table.
    """
    try:
        df = arrow_table.to_pandas()
        df.drop(columns=columns_to_delete, inplace=True, errors='ignore')
        new_table = pa.Table.from_pandas(df)
        return new_table
    except Exception as e:
        logger.error(f"Failed to drop columns {columns_to_delete}: {e}")
        raise