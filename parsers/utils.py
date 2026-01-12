"""
Utilitaires communs pour les parsers.
"""
import pandas as pd
import numpy as np


def sanitize_for_json(df: pd.DataFrame) -> list[dict]:
    """
    Nettoie un DataFrame pour le rendre JSON-ready.
    """
    df = df.replace([float("inf"), float("-inf"), np.inf, -np.inf], None)
    df = df.where(pd.notnull(df), None)

    clean_data = []
    for _, row in df.iterrows():
        clean_row = {}
        for col, val in row.items():
            if isinstance(val, float) and (pd.isna(val) or val in [float("inf"), float("-inf")]):
                clean_row[col] = None
            elif isinstance(val, str) and val.strip() == "":
                clean_row[col] = None
            else:
                clean_row[col] = val
        clean_data.append(clean_row)
    return clean_data

