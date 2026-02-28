import logging

import pandas as pd

logger = logging.getLogger('data-quality-service')

def basic_quality_checks(arrow_table, rules=None):
    """
    Perform data quality checks on an Arrow Table.

    Args:
      arrow_table (pyarrow.Table): The input data in Arrow format.
      rules (dict): Optional dictionary of rules:
        {
          "min_rows": int,             # minimum row count
          "check_null_ratio": bool,    # enable null ratio check
          "threshold_null_ratio": float, # max acceptable null ratio (0.0-1.0)
          "check_duplicates": bool,    # check for duplicate rows
          "check_column_types": dict,  # {"col_name": "numeric|string|datetime"}
          "check_unique_columns": list, # columns that should have all unique values
          "check_value_range": dict,   # {"col_name": {"min": N, "max": N}}
          "check_completeness": bool   # per-column completeness report
        }

    Returns:
      dict: Results with rows, cols, and checks dict.
    """
    df = arrow_table.to_pandas()
    n_rows, n_cols = df.shape
    result = {
        "rows": n_rows,
        "cols": n_cols,
        "checks": {}
    }

    if rules is None:
        rules = {}

    # ── Check: min_rows ──
    min_rows = rules.get("min_rows", 0)
    result["checks"]["min_rows"] = bool(n_rows >= min_rows)

    # ── Check: null_ratio ──
    if rules.get("check_null_ratio", False):
        threshold = rules.get("threshold_null_ratio", 0.5)
        total_cells = df.size
        total_null = df.isna().sum().sum()
        null_ratio = total_null / total_cells if total_cells > 0 else 0
        result["checks"]["null_ratio"] = {
            "value": float(null_ratio),
            "threshold": threshold,
            "pass": bool(null_ratio <= threshold)
        }

    # ── Check: duplicates ──
    if rules.get("check_duplicates", False):
        dup_count = int(df.duplicated().sum())
        result["checks"]["duplicates"] = {
            "duplicate_rows": dup_count,
            "pass": dup_count == 0
        }

    # ── Check: column types ──
    expected_types = rules.get("check_column_types", {})
    if expected_types:
        type_results = {}
        for col, expected in expected_types.items():
            if col not in df.columns:
                type_results[col] = {"expected": expected, "actual": "missing", "pass": False}
                continue
            actual_dtype = str(df[col].dtype)
            if expected == "numeric":
                ok = pd.api.types.is_numeric_dtype(df[col])
            elif expected == "string":
                ok = pd.api.types.is_string_dtype(df[col]) or pd.api.types.is_object_dtype(df[col])
            elif expected == "datetime":
                ok = pd.api.types.is_datetime64_any_dtype(df[col])
            else:
                ok = expected in actual_dtype
            type_results[col] = {"expected": expected, "actual": actual_dtype, "pass": bool(ok)}
        result["checks"]["column_types"] = type_results

    # ── Check: unique columns ──
    unique_cols = rules.get("check_unique_columns", [])
    if unique_cols:
        unique_results = {}
        for col in unique_cols:
            if col not in df.columns:
                unique_results[col] = {"pass": False, "reason": "column not found"}
                continue
            n_unique = df[col].nunique()
            n_non_null = int(df[col].notna().sum())
            unique_results[col] = {
                "unique_values": int(n_unique),
                "non_null_rows": n_non_null,
                "pass": bool(n_unique == n_non_null)
            }
        result["checks"]["unique_columns"] = unique_results

    # ── Check: value range ──
    range_rules = rules.get("check_value_range", {})
    if range_rules:
        range_results = {}
        for col, bounds in range_rules.items():
            if col not in df.columns:
                range_results[col] = {"pass": False, "reason": "column not found"}
                continue
            if not pd.api.types.is_numeric_dtype(df[col]):
                range_results[col] = {"pass": False, "reason": "column is not numeric"}
                continue
            col_min = float(df[col].min()) if df[col].notna().any() else None
            col_max = float(df[col].max()) if df[col].notna().any() else None
            ok = True
            if "min" in bounds and col_min is not None:
                ok = ok and col_min >= bounds["min"]
            if "max" in bounds and col_max is not None:
                ok = ok and col_max <= bounds["max"]
            range_results[col] = {
                "actual_min": col_min,
                "actual_max": col_max,
                "expected_min": bounds.get("min"),
                "expected_max": bounds.get("max"),
                "pass": bool(ok)
            }
        result["checks"]["value_range"] = range_results

    # ── Check: completeness report ──
    if rules.get("check_completeness", False):
        completeness = {}
        for col in df.columns:
            non_null = int(df[col].notna().sum())
            completeness[col] = {
                "non_null": non_null,
                "total": n_rows,
                "ratio": round(non_null / n_rows, 4) if n_rows > 0 else 0.0
            }
        result["checks"]["completeness"] = completeness

    return result
