from sqlalchemy import create_engine
import pandas as pd
import logging

logger = logging.getLogger('extract-sql-service')

def extract_from_sql(db_url, query):
    """
    Execute a SQL query using SQLAlchemy e return data like DataFrame.
    """
    try:
        engine = create_engine(db_url)
        with engine.connect() as connection:
            data = pd.read_sql(query, connection)
        return data
    except Exception as e:
        raise ConnectionError(f"Error during query execution: {e}")