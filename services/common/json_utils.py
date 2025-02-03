import json
import numpy as np

class NpEncoder(json.JSONEncoder):
    """
    JSON Encoder che converte automaticamente i tipi NumPy
    (int, float, array, bool, ecc.) nei corrispondenti tipi Python standard.
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