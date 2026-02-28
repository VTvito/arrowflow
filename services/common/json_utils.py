import json

import numpy as np


class NpEncoder(json.JSONEncoder):
    """
    JSON Encoder that convert automatically the NumPy types
    (int, float, array, bool, ecc.) in the same types of Python standard.
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
        # Let the base class default method
        return super(NpEncoder, self).default(obj)
