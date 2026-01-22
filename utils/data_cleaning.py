"""
Data cleaning utilities for LaCriee pipeline.

Provides functions for cleaning and sanitizing data before export to JSON
or other formats.
"""
import pandas as pd
import numpy as np
import re
from typing import List


def sanitize_for_json(df: pd.DataFrame) -> List[dict]:
    """
    Sanitize DataFrame for JSON serialization.

    Handles:
    - Infinity values (inf, -inf) -> None
    - NaN/NA values -> None
    - Empty strings -> None

    Args:
        df: pandas DataFrame to sanitize

    Returns:
        List of dictionaries with sanitized values
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


def is_prix(val: str) -> bool:
    """
    Check if a value is a valid price/number format.

    Matches patterns like:
    - "123"
    - "123.45"
    - "123,45"
    - "-123.45"

    Args:
        val: String value to check

    Returns:
        True if value matches price pattern, False otherwise
    """
    return re.match(r"^-?$|^\d+(?:[.,]\d+)?$", val) is not None
