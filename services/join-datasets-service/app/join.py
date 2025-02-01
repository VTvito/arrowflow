import pandas as pd

def join_datasets(file_paths, join_key, join_type):
    """
    Unisce più dataset sulla base di una chiave comune.

    Args:
        file_paths (list): Lista di percorsi ai file CSV.
        join_key (str): Chiave di join comune.
        join_type (str): Tipo di join ('inner', 'outer', 'left', 'right').

    Returns:
        pd.DataFrame: Dataset combinato.
    """
    # Carica il primo dataset
    combined_data = pd.read_csv(file_paths[0])

    # Unisci i dataset successivi
    for file_path in file_paths[1:]:
        data = pd.read_csv(file_path)
        combined_data = combined_data.merge(data, on=join_key, how=join_type)

    return combined_data