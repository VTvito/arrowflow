import json
import numpy as np

class NpEncoder(json.JSONEncoder):
    """
    JSON Encoder convert automatically NumPy types
    (int, float, array, ecc.) in type Python standard (int, float, list).
    """
    def default(self, obj):
        if isinstance(obj, (np.int_, np.int64, np.int32)):
            return int(obj)
        elif isinstance(obj, (np.float_, np.float32, np.float64)):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        # Let the base class default method
        return super(NpEncoder, self).default(obj)