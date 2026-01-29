"""
Utilitaires communs pour les parsers.
"""
import pandas as pd
import numpy as np
import re
from typing import Optional


# =============================================================================
# AFFINAGE DES CATÉGORIES GÉNÉRIQUES
# =============================================================================

# Patterns à EXCLURE de l'affinage (produits transformés ou non-marins)
EXCLUDE_PATTERNS = [
    r'\bSOUPE\b',
    r'\bPATE\b',
    r'\bARETE\b',
    r'\bCOCKTAIL\b',
    r'\bGRENOUILLE\b',
    r'\bESCARGOT\b',
    r'\bECREVISSE\b',  # Eau douce, pas de la mer
    r'\bPLATEAU\b',    # Plateau de fruits de mer
    r'\bASSIETTE\b',   # Assiette de la mer
]

# Mapping espèce → catégorie spécifique (ordre: plus spécifique d'abord)
SPECIES_TO_CATEGORY = [
    # Patterns composés (à tester en premier)
    (r'\bNOIX\s*(?:DE\s*)?(?:COQ\.?\s*)?(?:ST|SAINT)\s*JACQUES\b', 'NOIX ST JACQUES'),
    (r'\bCOQUILLE\s*(?:ST|SAINT)\s*JACQUES\b', 'COQUILLE ST JACQUES'),
    (r'\bLIEU\s*JAUNE\b', 'LIEU JAUNE'),
    (r'\bLIEU\s*NOIR\b', 'LIEU NOIR'),
    (r'\bROUGET\s*BARBET\b', 'ROUGET BARBET'),
    (r'\bSAINT\s*PIERRE\b', 'SAINT PIERRE'),
    (r'\bST\s*PIERRE\b', 'SAINT PIERRE'),

    # Crustacés
    (r'\bKING\s*CRAB\b', 'KING CRAB'),  # Pattern spécifique avant CRABE
    (r'\bTOURTEAUX?\b', 'TOURTEAU'),
    (r'\bHOMARDS?\b', 'HOMARD'),
    (r'\bARAIGN[EÉ]ES?\b', 'ARAIGNEE'),
    (r'\bLANGOUSTINES?\b', 'LANGOUSTINE'),
    (r'\bLANGOUSTES?\b', 'LANGOUSTE'),
    (r'\bCREVETTES?\b', 'CREVETTE'),
    (r'\bETRILLES?\b', 'ETRILLE'),
    (r'\bCRABE\b', 'CRABE'),

    # Coquillages
    (r'\bBULOTS?\b', 'BULOT'),
    (r'\bBIGORNEAUX?\b', 'BIGORNEAU'),
    (r'\bOURSINS?\b', 'OURSIN'),
    (r'\bPALOURDES?\b', 'PALOURDE'),
    (r'\bHUITRES?\b', 'HUITRE'),
    (r'\bCOQUES?\b', 'COQUE'),
    (r'\bCOUTEAUX?\b', 'COUTEAU'),
    (r'\bPRAIRES?\b', 'PRAIRE'),
    (r'\bORMEAUX?\b', 'ORMEAU'),
    (r'\bAMANDES?\b', 'AMANDE'),
    (r'\bVERNIS\b', 'VERNIS'),
    (r'\bTELLINES?\b', 'TELLINE'),
    (r'\bCLAMS?\b', 'CLAM'),
    (r'\bVENUS\b', 'VENUS'),
    (r'\bPOUCE[\s-]?PIEDS?\b', 'POUCE PIED'),
    (r'\bMOULES?\b', 'MOULE'),

    # Poissons
    (r'\bLIMANDE\b', 'LIMANDE'),
    (r'\bBONITE\b', 'BONITE'),
    (r'\bCHINCHARD\b', 'CHINCHARD'),
    (r'\bMULET\b', 'MULET'),
    (r'\bBAR\b', 'BAR'),
    (r'\bCABILLAUD\b', 'CABILLAUD'),
    (r'\bMERLU\b', 'MERLU'),
    (r'\bEGLEFIN\b', 'EGLEFIN'),
    (r'\bDORADE\b', 'DORADE'),
    (r'\bJULIENNE\b', 'JULIENNE'),
    (r'\bMAIGRE\b', 'MAIGRE'),
    (r'\bTACAUD\b', 'TACAUD'),
    (r'\bGRONDIN\b', 'GRONDIN'),
    (r'\bROUSSETTE\b', 'ROUSSETTE'),
    (r'\bMERLAN\b', 'MERLAN'),
    (r'\bSOLE\b', 'SOLE'),
    (r'\bTURBOT\b', 'TURBOT'),
    (r'\bRAIE\b', 'RAIE'),
    (r'\bLOTTE\b', 'LOTTE'),
    (r'\bBAUDROIE\b', 'LOTTE'),  # Baudroie = Lotte
    (r'\bCONGRE\b', 'CONGRE'),
    (r'\bSAUMON\b', 'SAUMON'),
    (r'\bTHON\b', 'THON'),
    (r'\bMAQUEREAU\b', 'MAQUEREAU'),
    (r'\bSARDINE\b', 'SARDINE'),
    (r'\bANCHOIS\b', 'ANCHOIS'),
    (r'\bROUGET\b', 'ROUGET'),
    (r'\bLINGUE\b', 'LINGUE'),
    (r'\bLIEU\b', 'LIEU'),
    (r'\bSAR\b', 'SAR'),
    (r'\bFLET\b', 'FLET'),

    # Céphalopodes
    (r'\bENCORNETS?\b', 'ENCORNET'),
    (r'\bSEICHES?\b', 'SEICHE'),
    (r'\bPOULPES?\b', 'POULPE'),
]


def refine_generic_category(
    categorie: Optional[str],
    product_name: Optional[str],
    generic_categories: set
) -> Optional[str]:
    """
    Affine une catégorie générique vers une espèce spécifique.

    Cette fonction est appelée par les parsers après extraction de la catégorie
    depuis le PDF/Excel. Si la catégorie est trop générique (ex: COQUILLAGES,
    DIVERS), elle analyse le product_name pour extraire l'espèce.

    Args:
        categorie: Catégorie actuelle (ex: "COQUILLAGES")
        product_name: Nom du produit (ex: "Tourteaux 800/1.2")
        generic_categories: Set des catégories à affiner pour ce vendor

    Returns:
        Catégorie affinée ou originale si pas d'affinage possible

    Examples:
        >>> refine_generic_category("COQUILLAGES", "Tourteaux 800/1.2", {"COQUILLAGES"})
        'TOURTEAU'
        >>> refine_generic_category("DIVERS", "POISSONS SOUPE", {"DIVERS"})
        'DIVERS'  # Garder car contient "SOUPE"
    """
    if not categorie or not product_name:
        return categorie

    cat_upper = categorie.upper().strip()

    # Ne pas affiner si pas une catégorie générique
    if cat_upper not in generic_categories:
        return categorie

    product_upper = product_name.upper()

    # Vérifier si c'est un produit à exclure (soupe, pâté, etc.)
    for exclude_pattern in EXCLUDE_PATTERNS:
        if re.search(exclude_pattern, product_upper):
            return categorie  # Garder la catégorie générique

    # Chercher l'espèce dans le nom du produit
    for pattern, species in SPECIES_TO_CATEGORY:
        if re.search(pattern, product_upper):
            return species

    return categorie


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

