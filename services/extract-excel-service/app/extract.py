import os

import pandas as pd
import pyarrow as pa
from common.path_utils import resolve_input_path


def process_excel(file_path):
    """
    Load Excel into DataFrame and return Arrow Table.
    """
    try:
        safe_file_path = resolve_input_path(file_path)

        ext = os.path.splitext(safe_file_path)[-1].lower()
        if ext not in ['.xls', '.xlsx']:
            raise ValueError(f"File format not supported: {ext}. Supported: .xls, .xlsx")

        df = pd.read_excel(safe_file_path)
        return pa.Table.from_pandas(df)
    except Exception as e:
        raise ValueError(f"Error during Excel processing: {str(e)}") from e
