import pandas as pd
import pyarrow as pa
import logging

logger = logging.getLogger('outlier-detection-service')

def detect_and_remove_outliers(arrow_table, column, z_threshold=3.0):
    """
    Simple outlier detection: 
    - Convert to Pandas
    - Compute mean, std for `column`
    - Filter rows where abs(value - mean) / std > z_threshold
    - Return (cleaned_table, num_removed)
    """
    df = arrow_table.to_pandas()
    if column not in df.columns:
        logger.warning(f"Column '{column}' not found in DataFrame. No outlier removal.")
        return pa.Table.from_pandas(df), 0

    # Count before rows
    before_rows = df.shape[0]
    mean_val = df[column].mean()
    std_val = df[column].std()

    # log
    logger.info(f"Outlier detection on column={column}, mean={mean_val}, std={std_val}, z_threshold={z_threshold}")
    if pd.isna(std_val) or std_val == 0:
        logger.warning(f"Std=0 or NaN => no outlier removal.")
        return pa.Table.from_pandas(df), 0

    # Row filter
    z_score = (df[column] - mean_val).abs() / std_val
    filtered_df = df[z_score <= z_threshold]
    removed_count = before_rows - filtered_df.shape[0]

    logger.info(f"Removed {removed_count} outliers on '{column}' with Z-score > {z_threshold}")

    new_table = pa.Table.from_pandas(filtered_df)
    return (new_table, removed_count)