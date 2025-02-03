import pyarrow as pa
import pandas as pd
import logging
import io

logger = logging.getLogger('load-data-service')

def load_arrow_to_format(arrow_table, format_type):
    """
    Converts an Arrow Table to the specified format ('csv','excel','json').
    Returns bytes of the converted data.
    """
    try:
        df = arrow_table.to_pandas()
        logger.info(f"Converted Arrow Table to Pandas DataFrame with {df.shape[0]} rows and {df.shape[1]} columns.")

        if format_type.lower() == 'csv':
            output = df.to_csv(index=False)
            logger.info("Converted DataFrame to CSV format.")
            return output.encode('utf-8')

        elif format_type.lower() == 'xlsx' or format_type.lower() == 'xls':
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                df.to_excel(writer, index=False, sheet_name='Sheet1')
            logger.info("Converted DataFrame to Excel format.")
            return output.getvalue()

        elif format_type.lower() == 'json':
            output = df.to_json(orient='records')
            logger.info("Converted DataFrame to JSON format.")
            return output.encode('utf-8')

        else:
            logger.error(f"Unsupported format: {format_type}")
            raise ValueError(f"Unsupported format: {format_type}")
    except Exception as e:
        logger.error(f"Failed to convert data: {e}")
        raise