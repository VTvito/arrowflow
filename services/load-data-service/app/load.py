import io
import logging

import pandas as pd
import pyarrow.parquet as pq

logger = logging.getLogger('load-data-service')

def load_arrow_to_format(arrow_table, format_type):
    """
    Converts an Arrow Table to the specified format ('csv','excel','json','parquet').
    Returns bytes of the converted data.
    """
    try:
        if not isinstance(format_type, str) or not format_type.strip():
            raise ValueError("Unsupported format: format_type must be a non-empty string")

        normalized_format = format_type.lower().strip()

        # Parquet can be written directly from Arrow without Pandas conversion
        if normalized_format == 'parquet':
            output = io.BytesIO()
            pq.write_table(arrow_table, output, compression='snappy')
            logger.info("Converted Arrow Table to Parquet format (snappy compression).")
            return output.getvalue()

        df = arrow_table.to_pandas()
        logger.info(f"Converted Arrow Table to Pandas DataFrame with {df.shape[0]} rows and {df.shape[1]} columns.")

        if normalized_format == 'csv':
            output = df.to_csv(index=False)
            logger.info("Converted DataFrame to CSV format.")
            return output.encode('utf-8')

        elif normalized_format == 'xlsx' or normalized_format == 'xls':
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                df.to_excel(writer, index=False, sheet_name='Sheet1')
            logger.info("Converted DataFrame to Excel format.")
            return output.getvalue()

        elif normalized_format == 'json':
            output = df.to_json(orient='records')
            logger.info("Converted DataFrame to JSON format.")
            return output.encode('utf-8')

        else:
            logger.error(f"Unsupported format: {format_type}")
            raise ValueError(f"Unsupported format: {format_type}")
    except Exception as e:
        logger.error(f"Failed to convert data: {e}")
        raise
