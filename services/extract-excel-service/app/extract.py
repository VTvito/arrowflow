import logging
import os

import pandas as pd
import pyarrow as pa
from common.path_utils import resolve_input_path

logger = logging.getLogger('extract-excel-service')

def process_excel(file_path):
    """
    Load Excel into DataFrame and return Arrow Table.
    """
    try:
        safe_file_path = resolve_input_path(file_path)

        # Verify that is a supported Excel (extension)
        ext = os.path.splitext(safe_file_path)[-1].lower()
        if ext not in ['.xls', '.xlsx']:
            raise ValueError(f"File format not supported: {ext}. Supported: .xls, .xlsx")

        # Load file Excel in DataFrame
        df = pd.read_excel(safe_file_path)
        # Convert pandas DataFrame to Arrow Table
        arrow_table = pa.Table.from_pandas(df)
        logger.info(
            f"Converted DataFrame to Arrow Table with "
            f"{arrow_table.num_rows} rows, {arrow_table.num_columns} columns."
        )
        return arrow_table
    except Exception as e:
        raise ValueError(f"Erro during Excel file elaboration: {str(e)}")
