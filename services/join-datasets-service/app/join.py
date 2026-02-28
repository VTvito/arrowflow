import pyarrow as pa

VALID_JOIN_TYPES = {"inner", "left", "right", "outer"}


def join_datasets_logic(table1, table2, join_key="id", join_type="inner"):
    """
    Execute the join of 2 datasets represented like Arrow Table.

    Args:
      table1 (pyarrow.Table): 1 table.
      table2 (pyarrow.Table): 2 table.
      join_key (str): Join's key.
      join_type (str): Join's type ("inner", "left", "right", "outer").

    Returns:
      tuple: (joined_table, (rows_out, cols_out))
             where joined_table is the output table in Arrow format,
             and (rows_out, cols_out) is the shape of the output table.
    """
    if not isinstance(join_key, str) or not join_key.strip():
        raise ValueError("join_key must be a non-empty string")

    normalized_join_type = str(join_type).strip().lower()
    if normalized_join_type not in VALID_JOIN_TYPES:
        raise ValueError(f"join_type must be one of {sorted(VALID_JOIN_TYPES)}")

    df1 = table1.to_pandas()
    df2 = table2.to_pandas()

    if join_key not in df1.columns:
        raise ValueError(f"join_key '{join_key}' not found in first dataset")

    if join_key not in df2.columns:
        raise ValueError(f"join_key '{join_key}' not found in second dataset")

    joined_df = df1.merge(df2, on=join_key, how=normalized_join_type)
    joined_table = pa.Table.from_pandas(joined_df)

    return joined_table, joined_df.shape
