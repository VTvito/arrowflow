import pyarrow.csv as pacsv
import logging

logger = logging.getLogger('extract-csv-service')

def load_csv_to_arrow(file_path):
    """
    Reads a CSV file and converts it into an Apache Arrow Table.
    
    Args:
        file_path (str): Path to the CSV file.
    
    Returns:
        pyarrow.Table: Table containing the CSV data in Arrow format.
    """
    try:
        table = pacsv.read_csv(file_path)
        logger.info(f"Loaded CSV file {file_path} into Arrow Table with {table.num_rows} rows.")
        return table
    except Exception as e:
        logger.error(f"Failed to load CSV into Arrow format: {e}")
        raise