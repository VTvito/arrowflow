import json

import numpy as np
import pandas as pd


class NpEncoder(json.JSONEncoder):
    """
    JSON Encoder that converts NumPy and Pandas types
    (int, float, array, bool, Timestamp, NA, etc.) to Python standard types.
    """
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.bool_):
            return bool(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, np.datetime64):
            return str(obj)
        elif isinstance(obj, pd.Timestamp):
            return obj.isoformat()
        elif obj is pd.NaT or (isinstance(obj, float) and np.isnan(obj)):
            return None
        elif isinstance(obj, type(pd.NA)):
            return None
        return super().default(obj)
