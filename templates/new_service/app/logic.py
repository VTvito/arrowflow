"""
{{SERVICE_NAME}} — Business Logic

Pure data transformation module. This file must NOT import Flask.
It receives a PyArrow Table (or Pandas DataFrame) and returns the result.

This separation allows unit testing without HTTP scaffolding.
"""

import pyarrow as pa


def process_data(table: pa.Table, params: dict) -> pa.Table:
    """
    Apply your transformation to the input Arrow Table.

    Args:
        table: Input data as a PyArrow Table.
        params: Dictionary of parameters from X-Params header.

    Returns:
        Transformed PyArrow Table.

    TODO: Implement your transformation logic here.
    """
    # Example: convert to Pandas, process, convert back
    # df = table.to_pandas()
    #
    # ... your transformation logic ...
    #
    # return pa.Table.from_pandas(df, preserve_index=False)

    # For now, pass-through (no-op)
    return table
