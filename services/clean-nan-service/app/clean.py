import logging

import pandas as pd
import pyarrow as pa

logger = logging.getLogger('clean-nan-service')

VALID_STRATEGIES = ("drop", "fill_mean", "fill_median", "fill_mode", "fill_value", "ffill", "bfill")


def apply_transformations(arrow_table, strategy="drop", fill_value=None, columns=None):
    """
    Cleans null/NaN values using the specified strategy.

    Args:
        arrow_table: Input pyarrow.Table.
        strategy: One of 'drop', 'fill_mean', 'fill_median', 'fill_mode',
                  'fill_value', 'ffill', 'bfill'.
        fill_value: Value to use when strategy='fill_value'.
        columns: Optional list of columns to apply the strategy to.
                 If None, applies to all columns (or all rows for 'drop').

    Returns:
        (cleaned_arrow_table, nulls_handled, total_null_before, total_cells)
    """
    if strategy not in VALID_STRATEGIES:
        raise ValueError(
            f"Invalid strategy '{strategy}'. Must be one of: {', '.join(VALID_STRATEGIES)}"
        )

    if strategy == "fill_value" and fill_value is None:
        raise ValueError("fill_value parameter is required when strategy is 'fill_value'")

    try:
        df = arrow_table.to_pandas()

        total_cells = df.size
        total_null_before = int(df.isna().sum().sum())

        # Determine target columns
        target_cols = columns if columns else df.columns.tolist()
        # Filter to only columns that actually exist
        target_cols = [c for c in target_cols if c in df.columns]

        if strategy == "drop":
            df_cleaned = df.dropna(subset=target_cols if columns else None)
        elif strategy == "fill_mean":
            for col in target_cols:
                if pd.api.types.is_numeric_dtype(df[col]):
                    df[col] = df[col].fillna(df[col].mean())
            df_cleaned = df
        elif strategy == "fill_median":
            for col in target_cols:
                if pd.api.types.is_numeric_dtype(df[col]):
                    df[col] = df[col].fillna(df[col].median())
            df_cleaned = df
        elif strategy == "fill_mode":
            for col in target_cols:
                mode_vals = df[col].mode()
                if not mode_vals.empty:
                    df[col] = df[col].fillna(mode_vals.iloc[0])
            df_cleaned = df
        elif strategy == "fill_value":
            df[target_cols] = df[target_cols].fillna(fill_value)
            df_cleaned = df
        elif strategy == "ffill":
            df[target_cols] = df[target_cols].ffill()
            df_cleaned = df
        elif strategy == "bfill":
            df[target_cols] = df[target_cols].bfill()
            df_cleaned = df

        total_null_after = int(df_cleaned.isna().sum().sum())
        nulls_handled = total_null_before - total_null_after

        cleaned_arrow_table = pa.Table.from_pandas(df_cleaned)
        return (cleaned_arrow_table, nulls_handled, total_null_before, total_cells)
    except Exception as e:
        logger.error(f"Failed to clean data: {e}")
        raise
