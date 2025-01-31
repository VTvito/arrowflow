import pandas as pd
import os
import pyarrow as pa
import pyarrow.ipc as pa_ipc
import logging

logger = logging.getLogger('extract-excel-service')

def process_excel(file_path):
    """
    Load Excel file and return like a DataFrame.
    """
    try:
        # Verify that is a supported Excel (extension)
        ext = os.path.splitext(file_path)[-1].lower()
        if ext not in ['.xls', '.xlsx']:
            raise ValueError(f"File format not supported: {ext}. Supported: .xls, .xlsx")
        
        # Load file Excel in DataFrame
        data = pd.read_excel(file_path)
        return data
    except Exception as e:
        raise ValueError(f"Erro during Excel file elaboration: {str(e)}")