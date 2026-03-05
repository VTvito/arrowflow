"""
Monolithic ETL Pipeline — Pure Pandas Baseline

Same transformation logic as the microservices pipeline,
but executed as a single in-process Pandas pipeline.

Used as a performance baseline for benchmarking against
the microservices architecture.
"""

import os
import time

import numpy as np
import pandas as pd


def run_monolithic_pipeline(
    file_path: str,
    output_path: str,
    output_format: str = "csv",
    z_threshold: float = 3.0,
    null_threshold: float = 0.5,
    columns_to_drop: list[str] | None = None,
    outlier_column: str = "MonthlyIncome",
) -> dict:
    """
    Execute the full ETL pipeline as a monolithic Pandas process.

    Steps (matching the microservices pipeline):
      1. Extract CSV
      2. Data quality checks
      3. Delete constant columns
      4. Outlier detection (z-score)
      5. Clean NaN
      6. Load/save result

    Returns:
        Dictionary with timing and stats for each step.
    """
    if columns_to_drop is None:
        columns_to_drop = ["EmployeeCount", "Over18", "StandardHours"]

    results = {}
    total_start = time.time()

    # ── Step 1: Extract CSV ──
    t0 = time.time()
    df = pd.read_csv(file_path)
    results["extract"] = {
        "duration_sec": time.time() - t0,
        "rows": len(df),
        "columns": len(df.columns),
    }

    # ── Step 2: Data Quality Check ──
    t0 = time.time()
    if len(df) < 10:
        raise ValueError(f"Dataset has only {len(df)} rows (min: 10)")
    total_cells = df.shape[0] * df.shape[1]
    total_nulls = df.isnull().sum().sum()
    null_ratio = total_nulls / total_cells if total_cells > 0 else 0
    if null_ratio > null_threshold:
        raise ValueError(f"Null ratio {null_ratio:.3f} exceeds threshold {null_threshold}")
    results["quality_check"] = {
        "duration_sec": time.time() - t0,
        "null_ratio": float(null_ratio),
        "rows": len(df),
    }

    # ── Step 3: Delete Columns ──
    t0 = time.time()
    existing_cols = [c for c in columns_to_drop if c in df.columns]
    df = df.drop(columns=existing_cols)
    results["delete_columns"] = {
        "duration_sec": time.time() - t0,
        "dropped": existing_cols,
        "remaining_columns": len(df.columns),
    }

    # ── Step 4: Outlier Detection ──
    t0 = time.time()
    if outlier_column in df.columns:
        col = df[outlier_column]
        mean = col.mean()
        std = col.std()
        if std > 0:
            z_scores = np.abs((col - mean) / std)
            outlier_mask = z_scores > z_threshold
            n_outliers = outlier_mask.sum()
            df = df[~outlier_mask]
        else:
            n_outliers = 0
    else:
        n_outliers = 0
    results["outlier_detection"] = {
        "duration_sec": time.time() - t0,
        "outliers_removed": int(n_outliers),
        "rows_remaining": len(df),
    }

    # ── Step 5: Clean NaN ──
    t0 = time.time()
    rows_before = len(df)
    df = df.dropna()
    rows_removed = rows_before - len(df)
    results["clean_nan"] = {
        "duration_sec": time.time() - t0,
        "rows_removed": rows_removed,
        "rows_remaining": len(df),
    }

    # ── Step 6: Load/Save ──
    t0 = time.time()
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    if output_format == "csv":
        df.to_csv(output_path, index=False)
    elif output_format == "xlsx":
        df.to_excel(output_path, index=False)
    elif output_format == "json":
        df.to_json(output_path, orient="records")
    results["load"] = {
        "duration_sec": time.time() - t0,
        "output_path": output_path,
        "output_size_bytes": os.path.getsize(output_path),
    }

    results["total_duration_sec"] = time.time() - total_start
    results["final_shape"] = {"rows": len(df), "columns": len(df.columns)}

    return results


if __name__ == "__main__":
    import argparse
    import json

    parser = argparse.ArgumentParser(description="Run monolithic ETL pipeline")
    parser.add_argument("--input", required=True, help="Input CSV path")
    parser.add_argument("--output", default="benchmark/results/mono_output.csv", help="Output path")
    parser.add_argument("--format", default="csv", choices=["csv", "xlsx", "json"])
    args = parser.parse_args()

    results = run_monolithic_pipeline(args.input, args.output, args.format)
    print(json.dumps(results, indent=2))
