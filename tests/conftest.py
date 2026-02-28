"""
Shared test fixtures for the ETL microservices test suite.
"""

import os
import sys

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

# Add project root to sys.path so imports work
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)
sys.path.insert(0, os.path.join(PROJECT_ROOT, "services"))
sys.path.insert(0, os.path.join(PROJECT_ROOT, "services", "common"))


@pytest.fixture
def sample_arrow_table():
    """A basic Arrow table for testing transform services."""
    df = pd.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
        "age": [30, 25, 35, 28, 42],
        "salary": [50000.0, 60000.0, 55000.0, 70000.0, 48000.0],
        "department": ["HR", "Engineering", "HR", "Engineering", "Marketing"],
    })
    return pa.Table.from_pandas(df)


@pytest.fixture
def sample_arrow_table_with_nulls():
    """Arrow table with null values for testing clean-nan service."""
    df = pd.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "name": ["Alice", None, "Charlie", "Diana", None],
        "age": [30, 25, None, 28, 42],
        "salary": [50000.0, None, 55000.0, 70000.0, 48000.0],
        "department": ["HR", "Engineering", "HR", None, "Marketing"],
    })
    return pa.Table.from_pandas(df)


@pytest.fixture
def sample_arrow_table_with_outliers():
    """Arrow table with outlier values for testing outlier detection."""
    np.random.seed(42)
    salaries = np.random.normal(60000, 5000, 100).tolist()
    salaries[10] = 999999.0  # extreme outlier
    salaries[50] = 1000.0    # extreme outlier

    df = pd.DataFrame({
        "id": list(range(100)),
        "name": [f"Employee_{i}" for i in range(100)],
        "salary": salaries,
    })
    return pa.Table.from_pandas(df)


@pytest.fixture
def large_arrow_table():
    """A large Arrow table (100k rows) for performance testing."""
    np.random.seed(42)
    n = 100_000
    df = pd.DataFrame({
        "id": range(n),
        "name": [f"Employee_{i}" for i in range(n)],
        "age": np.random.randint(20, 65, n),
        "salary": np.random.normal(60000, 15000, n).round(2),
        "department": np.random.choice(["HR", "Engineering", "Marketing", "Sales", "Finance"], n),
    })
    return pa.Table.from_pandas(df)


@pytest.fixture
def sample_ipc_data(sample_arrow_table):
    """Arrow IPC bytes from the sample table."""
    from common.arrow_utils import table_to_ipc
    return table_to_ipc(sample_arrow_table)


@pytest.fixture
def sample_ipc_data_with_nulls(sample_arrow_table_with_nulls):
    """Arrow IPC bytes from the table with nulls."""
    from common.arrow_utils import table_to_ipc
    return table_to_ipc(sample_arrow_table_with_nulls)


@pytest.fixture
def services_config():
    """Test services config pointing to localhost."""
    return {
        "extract_csv": "http://localhost:5001/extract-csv",
        "clean_nan": "http://localhost:5002/clean-nan",
        "delete_columns": "http://localhost:5004/delete-columns",
        "extract_sql": "http://localhost:5005/extract-sql",
        "extract_api": "http://localhost:5006/extract-api",
        "extract_excel": "http://localhost:5007/extract-excel",
        "join_datasets": "http://localhost:5008/join-datasets",
        "load_data": "http://localhost:5009/load-data",
        "data_quality": "http://localhost:5010/data-quality",
        "outlier_detection": "http://localhost:5011/outlier-detection",
        "text_completion_llm": "http://localhost:5012/text-completion-llm",
    }
