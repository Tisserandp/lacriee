"""
Parser pour Laurent-Daniel PDF.
"""
from io import BytesIO
import fitz
import pandas as pd
import numpy as np
import re
from datetime import date
from parsers.utils import sanitize_for_json
import logging

logger = logging.getLogger(__name__)


def parse(file_bytes: bytes) -> list[dict]:
    """
    Parse un PDF Laurent-Daniel et retourne des données RAW.
    
    Returns:
        Liste de dictionnaires avec: Date, ProductName, Code_Provider, Prix, Qualité, Catégorie
    """
    # Import de la fonction existante depuis main.py
    # Pour l'instant, on utilise la fonction existante
    from main import extract_LD_data_from_pdf
    
    # La fonction existante retourne déjà le bon format
    return extract_LD_data_from_pdf(file_bytes)

