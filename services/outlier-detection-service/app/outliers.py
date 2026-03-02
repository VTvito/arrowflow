import logging
import math

import pandas as pd
import pyarrow as pa

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

    try:
        z_threshold = float(z_threshold)
    except (TypeError, ValueError) as exc:
        raise ValueError("z_threshold must be a numeric value") from exc

    if not math.isfinite(z_threshold) or z_threshold <= 0:
        raise ValueError("z_threshold must be a positive finite number")

    series_numeric = pd.to_numeric(df[column], errors="coerce")
    if series_numeric.notna().sum() == 0:
        raise ValueError(f"Column '{column}' must contain numeric values for outlier detection")

    before_rows = df.shape[0]
    mean_val = series_numeric.mean()
    std_val = series_numeric.std()

    if pd.isna(std_val) or std_val == 0:
        logger.warning("Std=0 or NaN => no outlier removal.")
        return pa.Table.from_pandas(df), 0

    z_score = (series_numeric - mean_val).abs() / std_val
    # Keep rows where z_score is NaN to avoid dropping non-numeric rows silently
    keep_mask = (z_score <= z_threshold) | z_score.isna()
    filtered_df = df[keep_mask]
    removed_count = before_rows - filtered_df.shape[0]

    new_table = pa.Table.from_pandas(filtered_df)
    return (new_table, removed_count)
