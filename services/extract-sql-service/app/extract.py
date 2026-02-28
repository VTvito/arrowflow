import re

import pandas as pd
import pyarrow as pa
from sqlalchemy import create_engine
from sqlalchemy.engine import make_url

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

_ALLOWED_DB_SCHEMES = {"postgresql", "postgresql+psycopg2", "mysql", "mysql+pymysql",
                       "mssql", "mssql+pyodbc", "oracle", "oracle+cx_oracle"}


def extract_from_sql(db_url, query):
    """
    Execute a SQL query using SQLAlchemy put it into DataFrame and return Arrow Table.
    The query is validated before execution and only allow-listed DB schemes are accepted.
    """
    try:
        # Validate query
        safe_query = validate_sql_query(query)

        # Restrict database scheme to prevent file-based access (e.g. sqlite)
        parsed_url = make_url(db_url)
        scheme = parsed_url.get_backend_name()
        if scheme not in _ALLOWED_DB_SCHEMES:
            raise ValueError(
                f"Database scheme '{scheme}' is not allowed. "
                f"Allowed: {sorted(_ALLOWED_DB_SCHEMES)}"
            )

        engine = create_engine(db_url)
        with engine.connect() as connection:
            df = pd.read_sql(safe_query, connection)
        return pa.Table.from_pandas(df)
    except (ValueError, ConnectionError):
        raise
    except Exception as e:
        raise ConnectionError(f"Error during query execution: {e}") from e
