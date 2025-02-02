from sqlalchemy import create_engine
import pandas as pd
import pyarrow as pa
import logging

logger = logging.getLogger('extract-sql-service')

def extract_from_sql(db_url, query):
    """
    Execute a SQL query using SQLAlchemy put it into DataFrame and return Arrow Table.
    """
    try:
        engine = create_engine(db_url)
        with engine.connect() as connection:
            df = pd.read_sql(query, connection)
        arrow_table = pa.Table.from_pandas(df)
        logger.info(f"Converted DataFrame to Arrow Table with {arrow_table.num_rows} rows, {arrow_table.num_columns} columns.")
        return arrow_table
    except Exception as e:
        raise ConnectionError(f"Error during query execution: {e}")