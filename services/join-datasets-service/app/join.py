import pyarrow as pa
import pandas as pd

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
    # Convert Arrow Table to Pandas DataFrame
    df1 = table1.to_pandas()
    df2 = table2.to_pandas()
    
    # Perform the join operation in Pandas
    joined_df = df1.merge(df2, on=join_key, how=join_type)
    
    # Convert the joined DataFrame back to Arrow Table
    joined_table = pa.Table.from_pandas(joined_df)
    
    return joined_table, joined_df.shape