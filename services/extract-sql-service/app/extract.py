import logging
import re

import pandas as pd
import pyarrow as pa
from sqlalchemy import create_engine
from sqlalchemy.engine import make_url

logger = logging.getLogger('extract-sql-service')

READ_ONLY_SQL_PATTERN = re.compile(r"^\s*(select|with)\b", re.IGNORECASE)
FORBIDDEN_SQL_PATTERN = re.compile(
    r"\b(insert|update|delete|drop|alter|truncate|create|grant|revoke|merge|call|copy)\b",
    re.IGNORECASE,
)


def validate_sql_query(query):
    if not isinstance(query, str) or not query.strip():
        raise ValueError("Parameter 'query' must be a non-empty string")

    normalized = query.strip()

    if ";" in normalized.rstrip(";"):
        raise ValueError("Multiple SQL statements are not allowed")

    if not READ_ONLY_SQL_PATTERN.match(normalized):
        raise ValueError("Only read-only SELECT/WITH queries are allowed")

    if FORBIDDEN_SQL_PATTERN.search(normalized):
        raise ValueError("Potentially dangerous SQL keywords detected")

    return normalized.rstrip(";")


def redact_db_url(db_url):
    try:
        parsed = make_url(db_url)
        if parsed.password:
            parsed = parsed.set(password=None)
        return str(parsed)
    except Exception:
        return "invalid-db-url"

def extract_from_sql(db_url, query):
    """
    Execute a SQL query using SQLAlchemy put it into DataFrame and return Arrow Table.
    """
    try:
        safe_query = validate_sql_query(query)
        engine = create_engine(db_url)
        with engine.connect() as connection:
            df = pd.read_sql(safe_query, connection)
        arrow_table = pa.Table.from_pandas(df)
        logger.info(
            f"Converted DataFrame to Arrow Table with "
            f"{arrow_table.num_rows} rows, {arrow_table.num_columns} columns."
        )
        return arrow_table
    except Exception as e:
        raise ConnectionError(f"Error during query execution: {e}")
