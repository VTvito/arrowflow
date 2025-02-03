import pandas as pd
import pyarrow as pa
import logging

logger = logging.getLogger('data-quality-service')

def basic_quality_checks(arrow_table, rules=None):
    """
    Perform basic data quality checks on an Arrow Table.
    
    Args:
      arrow_table (pyarrow.Table): The input data in Arrow format.
      rules (dict): (Optional) dictionary of rules, e.g.
                    {
                      "min_rows": 1,
                      "check_null_ratio": true,
                      "threshold_null_ratio": 0.5
                    }
    Returns:
      dict: A dictionary with the results of the checks.
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
    
    # Example rule: min_rows
    min_rows = rules.get("min_rows", 0)
    result["checks"]["min_rows"] = (n_rows >= min_rows)
    
    # Example rule: check_null_ratio
    if rules.get("check_null_ratio", False):
        threshold = rules.get("threshold_null_ratio", 0.5)
        total_cells = df.size
        total_null = df.isna().sum().sum()
        null_ratio = total_null / total_cells if total_cells > 0 else 0
        result["checks"]["null_ratio"] = {
            "value": null_ratio,
            "threshold": threshold,
            "pass": null_ratio <= threshold
        }
    
    return result