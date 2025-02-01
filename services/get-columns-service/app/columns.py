import pandas as pd

def get_columns_from_csv(file_path):
    """
    Funzione che carica un CSV e restituisce la lista delle colonne.
    """
    df = pd.read_csv(file_path)
    return df.columns.tolist()