import logging

import pyarrow as pa

logger = logging.getLogger('delete-columns-service')

def drop_columns_arrow(arrow_table, columns_to_delete):
    """
    Remove specified columns from an Arrow Table, returning (new_table, removed_count).

    Uses native PyArrow ``drop_columns`` (zero-copy) instead of converting
    to Pandas and back.
    """
    try:
        if columns_to_delete is None:
            columns_to_delete = []
        elif isinstance(columns_to_delete, str):
            columns_to_delete = [columns_to_delete]
        elif not isinstance(columns_to_delete, (list, tuple, set)):
            raise ValueError("columns_to_delete must be a list-like collection or string")

        existing_cols = set(arrow_table.column_names)
        to_remove = [c for c in columns_to_delete if c in existing_cols]

        new_table = arrow_table.drop_columns(to_remove)
        return new_table, len(to_remove)
    except Exception as e:
        logger.error(f"Failed to drop columns {columns_to_delete}: {e}")
        raise

