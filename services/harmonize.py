"""
Module d'harmonisation des attributs pour les 5 parseurs LaCriee.

Ce module centralise toutes les règles de normalisation définies dans
docs/harmonisation_attributs.md

Usage:
    from services.harmonize import harmonize_product

    product = {...}  # Sortie brute d'un parseur
    harmonized = harmonize_product(product, vendor="Audierne")
"""
import re
import unicodedata
from typing import Optional


# =============================================================================
# MAPPINGS DE NORMALISATION
# =============================================================================

# --- Categorie ---
CATEGORIE_MAPPING = {
    # Variations vers valeur harmonisée (garder la spécificité)
    "PLIE/ CARRELET": "CARRELET",
    "PLIE/CARRELET": "CARRELET",
    "CRUSTACES BRETONS": "CRUSTACES",
    "CRUSTACES CUITS PAST": "CRUSTACES",
    # Normalisation singulier (garder l'espèce spécifique)
    "TOURTEAUX": "TOURTEAU",
    "ARAIGNEES": "ARAIGNEE",
    "HOMARDS": "HOMARD",
    "LANGOUSTINES": "LANGOUSTINE",
    "LANGOUSTES": "LANGOUSTE",
    "HUITRES": "HUITRE",
    "BULOTS": "BULOT",
    "BIGORNEAUX": "BIGORNEAU",
    "PALOURDES": "PALOURDE",
    "OURSINS": "OURSIN",
    "ST PIERRE": "SAINT PIERRE",
    "SAUMONS": "SAUMON",
    "LIEU": "LIEU JAUNE",
    "LIMANDE SOLE": "LIMANDE",
    "DIVERS POISSONS": "DIVERS",
    # Catégories FILET → à traiter spécialement (extraire espèce)
    "FILET DE POISSONS": None,  # Sera traité par extract_species_from_filet
    "FILETS": None,
    "BAR FILET": None,
}

# --- Methode_Peche ---
METHODE_PECHE_MAPPING = {
    "PT BATEAU": "PB",
    "PETIT BATEAU": "PB",
    # "SAUVAGE" → traité spécialement (déplacer vers type_production)
}

# Valeurs à extraire de methode_peche vers d'autres champs
METHODE_PECHE_EXTRACT = {
    "SAUVAGE": {"field": "type_production", "value": "SAUVAGE"},
}

# --- Qualite ---
QUALITE_MAPPING = {
    "QUALITE PREMIUM": "PREMIUM",
}

# --- Etat ---
ETAT_MAPPING = {
    "VIDEE": "VIDE",
    "VIDÉ": "VIDE",
    "PELEE": "PELE",
    "PELÉE": "PELE",
    "CORAILLEES": "CORAILLE",
    "CORAIL": "CORAILLE",
    "DESARETEE": "DESARETE",
    "ENTIÈRE": "ENTIER",
    "ENTIERE": "ENTIER",
}

# Couleurs à extraire vers le champ 'couleur'
ETAT_COULEURS = {"ROUGE", "BLANCHE", "NOIRE"}

# --- Origine ---
ORIGINE_MAPPING = {
    "BRETON": "BRETAGNE",
    "VAT": "ATLANTIQUE",
    "VDK": "DANEMARK",
    "AQ": "AQUACULTURE",
    "ECOS": "ECOSSE",
    "IRL": "IRLANDE",
    "VDA": "AUDIERNE",
}

# Origines à extraire vers type_production
ORIGINE_EXTRACT = {
    "AQUACULTURE": {"field": "type_production", "value": "ELEVAGE"},
    "AQ": {"field": "type_production", "value": "ELEVAGE"},
}

# --- Conservation ---
CONSERVATION_MAPPING = {
    "CONGELEE": "CONGELE",
    "SURGELEE": "SURGELE",
}

# --- Trim ---
TRIM_MAPPING = {
    "TRIM C": "TRIM_C",
    "TRIM D": "TRIM_D",
    "TRIM E": "TRIM_E",
    "TRIM B": "TRIM_B",
}

# =============================================================================
# MAPPINGS SPÉCIFIQUES DEMARNE
# =============================================================================

# Labels officiels reconnus
DEMARNE_LABELS = {"MSC", "BIO", "ASC", "LABEL ROUGE", "IGP", "AOP"}

# Espèces à extraire depuis les catégories Demarne
DEMARNE_SPECIES_PATTERNS = [
    # Catégories composites → espèce
    (r'^SAUMON\b', 'SAUMON'),
    (r'^BAR\b', 'BAR'),
    (r'^DORADE\s+GRISE\b', 'DORADE GRISE'),
    (r'^DORADE\b', 'DORADE'),
    (r'^CREVETTE\b', 'CREVETTES'),
    (r'^HOMARD\b', 'HOMARD'),
    (r'^LANGOUSTE\b', 'LANGOUSTE'),
    (r'^LANGOUSTINE\b', 'LANGOUSTINE'),
    (r'^LOTTE\b', 'LOTTE'),
    (r'^TURBOT\b', 'TURBOT'),
    (r'^SAINT\s*PIERRE\b', 'SAINT PIERRE'),
    (r'^CONGRE\b', 'CONGRE'),
    (r'^MAIGRE\b', 'MAIGRE'),
    (r'^ENCORNET\b', 'ENCORNET'),
    (r'^POULPE\b', 'POULPE'),
    (r'^SEICHE\b', 'SEICHE'),
    (r'^MOULE', 'MOULES'),
    (r'^TOURTEAU\b', 'TOURTEAU'),
    (r'^THON\b', 'THON'),
    (r'^SANDRE\b', 'SANDRE'),
    (r'^TRUITE\b', 'TRUITE'),
    (r'^SOLE\b', 'SOLE'),
    (r'^PAGEOT\b', 'PAGEOT'),
    (r'^AILE DE RAIE\b', 'RAIE'),
    (r'^COQUILLE\s*SAINT\s*JACQUES\b', 'COQUILLE ST JACQUES'),
    (r'^NOIX DE ST JACQUES\b', 'NOIX ST JACQUES'),
    # Catégories huîtres (marques)
    (r'^HUITRE', 'HUITRES'),
    (r'^LA BELON\b', 'HUITRES'),
    (r'^LA CELTIQUE\b', 'HUITRES'),
    (r'^LA FINE\b', 'HUITRES'),
    (r'^LA PERLE NOIRE\b', 'HUITRES'),
    (r'^LA SPECIALE\b', 'HUITRES'),
    (r'^PLATE DE BRETAGNE\b', 'HUITRES'),
    (r'^SPECIALE', 'HUITRES'),
    (r'^KYS\b', 'HUITRES'),
    (r'^ETOILE\b', 'HUITRES'),
    # Catégories génériques
    (r'^COQUILLAGES', 'COQUILLAGES'),
    (r'^CRUSTACES', 'CRUSTACES'),
    # Catégories FILET → sera traité spécialement par extract_species_from_filet()
    # NE PAS matcher ici, laisser la logique spéciale prendre le relais
]

# =============================================================================
# PATTERNS POUR EXTRACTION D'ESPÈCE DEPUIS CATÉGORIES FILET
# =============================================================================

# Pattern pour catégorie spécifique: FILET(S) DE|D' {ESPECE}
# Ex: "FILET DE TRUITE", "FILETS DE BAR ÉLEVAGE", "FILETS D'ANCHOIS"
FILET_CAT_SPECIES_PATTERN = r"FILETS?\s+(?:DE\s+|D')([A-Z]+)"

# Pattern pour catégories génériques (chercher l'espèce dans variante)
# Ex: "FILETS POISSON BLANC", "FILETS POISSON BLEU", "POISSONS FILETS"
FILET_CAT_GENERIC_PATTERN = r'(FILETS?\s+POISSON\s+(BLANC|BLEU)|POISSONS?\s+FILETS?)'

# Pattern pour extraire l'espèce depuis variante
# Ex: "Filet cabillaud", "Filet de merlu", "Filet d'églefin"
FILET_VAR_SPECIES_PATTERN = r"FILET\s+(?:DE\s+|D')?([A-Za-z]+(?:\s+[A-Za-z]+)?)"

# Pattern alternatif pour variantes sans "Filet" (ex: "Aile de Raie", "Pavé de Morue")
FILET_VAR_ALT_PATTERN = r'(?:PAVE|AILE)\s+DE\s+([A-Z]+)'

# Mapping des espèces extraites depuis les catégories FILET vers valeurs normalisées
FILET_SPECIES_NORMALIZE = {
    'CABILLAUD': 'CABILLAUD',
    'LIEU NOIR': 'LIEU NOIR',
    'LIEU JAUNE': 'LIEU JAUNE',
    'LIEU': 'LIEU JAUNE',
    'EGLEFIN': 'EGLEFIN',
    'MERLAN': 'MERLAN',
    'MERLU': 'MERLU',
    'SEBASTE': 'SEBASTE',
    'LOUP': 'LOUP DE MER',
    'JULIENNE': 'JULIENNE',
    'LINGUE': 'LINGUE',
    'LINGUE BLEUE': 'LINGUE',
    'FLETAN': 'FLETAN',
    'TACAUD': 'TACAUD',
    'SABRE': 'SABRE',
    'PLIE': 'PLIE',
    'PERCHE': 'PERCHE DU NIL',
    'PERCHE DU NIL': 'PERCHE DU NIL',
    'THON': 'THON',
    'ESPADON': 'ESPADON',
    'MAQUEREAU': 'MAQUEREAU',
    'SARDINE': 'SARDINE',
    'TRUITE': 'TRUITE',
    'BAR': 'BAR',
    'DORADE GRISE': 'DORADE GRISE',
    'DORADE': 'DORADE',
    'ANCHOIS': 'ANCHOIS',
    'HARENG': 'HARENG',
    'HARENGS': 'HARENG',
    'SANDRE': 'SANDRE',
    'LOTTE': 'LOTTE',
    'ROUGET': 'ROUGET',
    'ROUGET BARBET': 'ROUGET BARBET',
    'SOLE': 'SOLE',
    'RAIE': 'RAIE',
    'SAUMON': 'SAUMON',
    'TURBOT': 'TURBOT',
    'MORUE': 'MORUE',
    # Especes pour categories generiques
    'COLIN': 'COLIN',
    'COLIN ALASKA': 'COLIN',
    'BROCHET': 'BROCHET',
    'PAGEOT': 'PAGEOT',
    'PAGRE': 'PAGRE',
    'GRONDIN': 'GRONDIN',
    'CARRELET': 'CARRELET',
    'LIMANDE': 'LIMANDE',
    'BARBUE': 'BARBUE',
    'CHINCHARD': 'CHINCHARD',
    'EPERLAN': 'EPERLAN',
    'MERLUCHON': 'MERLUCHON',
    'MEROU': 'MEROU',
    'MEROU BADECHE': 'MEROU',
    'MEROU THIOFF': 'MEROU',
    'MEROU A POINTS BLEUS': 'MEROU',
    'BARRACUDA': 'BARRACUDA',
    'RASCASSE': 'RASCASSE',
    'RASCASSE ROUGE': 'RASCASSE',
    'HOKI': 'HOKI',
    'REQUIN': 'REQUIN',
    'REQUIN PEAU BLEUE': 'REQUIN',
    'MAHI': 'MAHI MAHI',
    'MAHI MAHI': 'MAHI MAHI',
    'BADECHE': 'BADECHE',
    'BADECHE ROUGE': 'BADECHE',
    'DORADE CORYPHENE': 'MAHI MAHI',
    'OMBLE': 'OMBLE CHEVALIER',
    'OMBLE CHEVALIER': 'OMBLE CHEVALIER',
    'MULET': 'MULET',
    'SAUMONETTE': 'SAUMONETTE',
    'SAUMONETTE EMISSOLE': 'SAUMONETTE',
}

# Origines à extraire depuis les catégories Demarne
DEMARNE_ORIGINE_PATTERNS = [
    (r'\bNORVEGE\b', 'NORVEGE'),
    (r'\bNORV[EÈ]GE\b', 'NORVEGE'),
    (r'\bECOSSE\b', 'ECOSSE'),
    (r'\b[EÉ]COSSE\b', 'ECOSSE'),
    (r'\bBRETAGNE\b', 'BRETAGNE'),
    (r'\bMADAGASCAR\b', 'MADAGASCAR'),
    (r'\bCANADIEN\b', 'CANADA'),
    (r'\bEUROPEEN\b', 'EUROPE'),
]

# Type production à extraire depuis les catégories Demarne
DEMARNE_TYPE_PRODUCTION_PATTERNS = [
    (r'\bSAUVAGE\b', 'SAUVAGE'),
    (r'\bELEVAGE\b', 'ELEVAGE'),
    (r'\b[EÉ]LEVAGE\b', 'ELEVAGE'),
]

# Qualités à extraire depuis les catégories Demarne
DEMARNE_QUALITE_PATTERNS = [
    (r'\bSUPERIEUR\b', 'SUP'),
    (r'\bSUP[EÉ]RIEUR\b', 'SUP'),
    (r'\bPREMIUM\b', 'PREMIUM'),
    (r'\bLABEL ROUGE\b', 'LABEL ROUGE'),
]

# États à extraire depuis les catégories/variantes Demarne
DEMARNE_ETAT_PATTERNS = [
    (r'\bENTIER\b', 'ENTIER'),
    (r'\bENTI[EÈ]RE?\b', 'ENTIER'),
    (r'\bVIDE\b', 'VIDE'),
    (r'\bVID[EÉ]\b', 'VIDE'),
    (r'\bGRATTE\b', 'GRATTE'),
    (r'\bCUIT\b', 'CUIT'),
    (r'\bCUITE\b', 'CUIT'),
    (r'\bVIVANT\b', 'VIVANT'),
    (r'\bFUME\b', 'FUME'),
    (r'\bFUM[EÉ]\b', 'FUME'),
    (r'\bDECORTIQUE', 'DECORTIQUE'),
]

# États de préparation à extraire pour le champ decoupe
# Ces patterns détectent les états de préparation dans les noms de produits
# qui seront combinés avec les découpes physiques (FILET, DOS, etc.)
PREPARATION_STATE_PATTERNS = [
    (r'\bNON\s+VID[EÉ]E?S?\b', 'Non vidé'),      # Vérifier composés en premier
    (r'\bENTI[EÈ]RE?S?\b', 'Entier'),            # ENTIER, ENTIERE, ENTIÈRE
    (r'\bVID[EÉ]E?S?\b', 'Vidé'),                # VIDE, VIDÉ, VIDEE, VIDÉE
    (r'\bGRATT[EÉ]E?S?\b', 'Gratté'),            # GRATTE, GRATTÉ, GRATTEE
    (r'\b[EÉ]T[EÊ]T[EÉ]E?S?\b', 'Étêté'),        # ETETE, ÉTÊTÉ, ETETEE
    (r'\b[EÉ]CAILL[EÉ]E?S?\b', 'Écaillé'),       # ECAILLE, ÉCAILLÉ
    (r'\bPAR[EÉ]E?S?\b', 'Paré'),                # PARE, PARÉ, PAREE
    (r'\b[EÉ]VISC[EÉ]R[EÉ]E?S?\b', 'Éviscéré'),  # EVISCERE, ÉVISCÉRÉ
]

# Découpes à extraire depuis les variantes Demarne
DEMARNE_DECOUPE_PATTERNS = [
    (r'\bFILETS?\b', 'FILET'),  # Singulier et pluriel
    (r'\bDOS\b', 'DOS'),
    (r'\bQUEUE\b', 'QUEUE'),
    (r'\bPAVE\b', 'PAVE'),
    (r'\bPAV[EÉ]\b', 'PAVE'),
    (r'\bLONGE\b', 'LONGE'),
    (r'\bAILE\b', 'AILE'),
    (r'\bNOIX\b', 'NOIX'),
    (r'\bPINCE\b', 'PINCE'),
    (r'\bDARNE\b', 'DARNE'),
    (r'\bSTEAK\b', 'STEAK'),
]

# Catégories génériques Demarne où l'espèce doit être extraite de la variante
DEMARNE_GENERIC_CATEGORIES = {
    'DOS',
    'AUTRES POISSONS',
    'POISSON PLAT',
    'POISSON ENTIER',
    'PETIT POISSON',
    'POISSON DE ROCHE',
    'POISSONS EXOTIQUES',
    "POISSONS D'EAU DOUCE",
}

# Patterns pour extraire espèce + découpe depuis variante
# Ex: "Dos de cabillaud", "Filet de merlu", "Pavé de HOKI"
VARIANTE_DECOUPE_PATTERNS = [
    (r'DOS\s+(?:DE\s+|D\')([A-Za-zÀ-ÿ]+(?:\s+[A-Za-zÀ-ÿ]+)?)', 'DOS'),
    (r'FILETS?\s+(?:DE\s+|D\')([A-Za-zÀ-ÿ]+(?:\s+[A-Za-zÀ-ÿ]+)?)', 'FILET'),
    (r'PAVE\s+(?:DE\s+)?([A-Za-zÀ-ÿ]+(?:\s+[A-Za-zÀ-ÿ]+)?)', 'PAVE'),
    (r'STEAK\s+([A-Za-zÀ-ÿ]+)', 'STEAK'),
]

# Origines Demarne à normaliser (corrections orthographiques et variations)
DEMARNE_ORIGINE_MAPPING = {
    "ECOSSE": "ECOSSE",
    "ÉCOSSE": "ECOSSE",
    "DANNEMARK": "DANEMARK",
    "NORVEGE": "NORVEGE",
    "NORVÈGE": "NORVEGE",
    "ANE": "ATLANTIQUE N-EST",
    "AML": "MADAGASCAR",
    "UK": "ROYAUME-UNI",
    "UK - DK": "ROYAUME-UNI, DANEMARK",
    "USA": "USA",
    "U.S.A": "USA",
    "MED": "MEDITERRANEE",
}


# =============================================================================
# FONCTIONS DE NORMALISATION
# =============================================================================

def remove_accents(text: str) -> str:
    """Supprime les accents d'une chaîne."""
    if not text:
        return text
    nfkd = unicodedata.normalize('NFD', text)
    return ''.join(c for c in nfkd if unicodedata.category(c) != 'Mn')


def normalize_value(value: Optional[str]) -> Optional[str]:
    """
    Normalisation de base d'une valeur:
    - Majuscules
    - Suppression des accents
    - Strip des espaces
    """
    if value is None or value == "":
        return None
    value = str(value).strip().upper()
    value = remove_accents(value)
    return value if value else None


def normalize_calibre(calibre: Optional[str]) -> Optional[str]:
    """
    Normalise un calibre selon les règles définies:
    - Virgule → point (séparateur décimal)
    - Format "plus" unifié (500/+ → 500+, +2 → 2+)
    """
    if not calibre:
        return None

    calibre = str(calibre).strip()

    # Normaliser séparateur décimal (virgule → point)
    # Attention: ne pas toucher aux virgules dans les plages comme "1,5/2"
    # On remplace seulement quand c'est clairement un décimal
    calibre = re.sub(r'(\d),(\d)', r'\1.\2', calibre)

    # Normaliser format "plus"
    # 500/+ → 500+
    calibre = re.sub(r'(\d+)/\+', r'\1+', calibre)
    # +2 → 2+ (moins fréquent)
    calibre = re.sub(r'^\+(\d+)$', r'\1+', calibre)

    return calibre


def normalize_categorie(categorie: Optional[str], product_name: Optional[str] = None) -> dict:
    """
    Normalise une catégorie et gère les cas spéciaux (FILET).

    FILET peut être:
    - Une découpe (ex: "Filet de Bar" → decoupe=FILET)
    - Une méthode de pêche (ex: "Bar de Filet" → methode_peche=FILET)

    La distinction se fait par la position de FILET par rapport à l'espèce.

    Returns:
        dict avec 'categorie', 'decoupe_from_categorie', 'methode_peche_from_categorie'
    """
    result = {
        "categorie": None,
        "decoupe_from_categorie": None,
        "methode_peche_from_categorie": None
    }

    if not categorie:
        # Si catégorie vide mais product_name contient FILET → analyser le contexte
        if product_name and 'FILET' in product_name.upper():
            filet_meaning = determine_filet_meaning(product_name)
            if filet_meaning["is_methode_peche"]:
                result["methode_peche_from_categorie"] = "FILET"
            else:
                result["decoupe_from_categorie"] = "FILET"
            if filet_meaning["species"]:
                result["categorie"] = filet_meaning["species"]
            else:
                species = extract_species_from_name(product_name)
                if species:
                    result["categorie"] = species
        return result

    categorie = normalize_value(categorie)

    # Cas spécial: catégories contenant FILET
    if "FILET" in categorie:
        # Analyser la position de FILET par rapport à l'espèce
        # Priorité: analyser la catégorie, puis le product_name si besoin
        filet_meaning = determine_filet_meaning(categorie)

        if filet_meaning["is_methode_peche"]:
            # FILET après espèce = méthode de pêche (ex: "BAR FILET")
            result["methode_peche_from_categorie"] = "FILET"
            if filet_meaning["species"]:
                result["categorie"] = filet_meaning["species"]
        else:
            # FILET avant espèce ou seul = découpe (ex: "FILET DE BAR", "FILETS")
            result["decoupe_from_categorie"] = "FILET"
            # Extraire l'espèce depuis la catégorie ou le product_name
            if filet_meaning["species"]:
                result["categorie"] = filet_meaning["species"]
            elif product_name:
                species = extract_species_from_name(product_name)
                if species:
                    result["categorie"] = species
        return result

    # Mapping standard
    if categorie in CATEGORIE_MAPPING:
        mapped = CATEGORIE_MAPPING[categorie]
        if mapped is not None:
            result["categorie"] = mapped
        return result

    result["categorie"] = categorie

    # Affiner l'espèce avec le product_name si nécessaire
    if product_name and result["categorie"]:
        product_upper = remove_accents(product_name.upper().strip())

        # Si l'espèce est DORADE (ou contient DORADE) et le nom contient GRISE → DORADE GRISE
        if result["categorie"] in ("DORADE", "DORADE / PAGRE") or "DORADE" in result["categorie"]:
            if "GRISE" in product_upper:
                result["categorie"] = "DORADE GRISE"

    return result


def normalize_methode_peche(methode: Optional[str]) -> dict:
    """
    Normalise une méthode de pêche et extrait les champs additionnels.

    Returns:
        dict avec 'methode_peche', 'type_production', 'technique_abattage'
    """
    result = {
        "methode_peche": None,
        "type_production": None,
        "technique_abattage": None,
    }

    if not methode:
        return result

    methode = normalize_value(methode)

    # Cas spéciaux avec extraction
    if methode in METHODE_PECHE_EXTRACT:
        extract = METHODE_PECHE_EXTRACT[methode]
        result[extract["field"]] = extract["value"]
        if "replace_with" in extract:
            result["methode_peche"] = extract["replace_with"]
        return result

    # Mapping standard
    if methode in METHODE_PECHE_MAPPING:
        result["methode_peche"] = METHODE_PECHE_MAPPING[methode]
    else:
        result["methode_peche"] = methode

    return result


def normalize_etat(etat: Optional[str]) -> dict:
    """
    Normalise un état et extrait la couleur si applicable.

    Returns:
        dict avec 'etat' et 'couleur'
    """
    result = {"etat": None, "couleur": None}

    if not etat:
        return result

    etat = normalize_value(etat)

    # Cas spécial: couleurs → champ dédié
    if etat in ETAT_COULEURS:
        result["couleur"] = etat
        return result

    # Mapping standard
    if etat in ETAT_MAPPING:
        result["etat"] = ETAT_MAPPING[etat]
    else:
        result["etat"] = etat

    return result


def normalize_origine(origine: Optional[str]) -> dict:
    """
    Normalise une origine et extrait type_production si applicable.

    Returns:
        dict avec 'origine' et 'type_production'
    """
    result = {"origine": None, "type_production": None}

    if not origine:
        return result

    # Gérer les origines multiples (séparées par virgule)
    origines = [o.strip() for o in str(origine).split(",")]
    normalized_origines = []

    for orig in origines:
        orig = normalize_value(orig)
        if not orig:
            continue

        # Cas spécial: extraction vers type_production
        if orig in ORIGINE_EXTRACT:
            extract = ORIGINE_EXTRACT[orig]
            result["type_production"] = extract["value"]
            continue

        # Mapping standard
        if orig in ORIGINE_MAPPING:
            normalized_origines.append(ORIGINE_MAPPING[orig])
        else:
            normalized_origines.append(orig)

    if normalized_origines:
        result["origine"] = ", ".join(normalized_origines)

    return result


def normalize_qualite(qualite: Optional[str]) -> Optional[str]:
    """Normalise une qualité."""
    if not qualite:
        return None

    qualite = normalize_value(qualite)
    return QUALITE_MAPPING.get(qualite, qualite)


def normalize_decoupe(decoupe: Optional[str]) -> Optional[str]:
    """Normalise une découpe."""
    if not decoupe:
        return None

    decoupe = normalize_value(decoupe)
    # Mapping FT → FILET
    if decoupe == "FT":
        return "FILET"
    return decoupe


def normalize_conservation(conservation: Optional[str]) -> Optional[str]:
    """Normalise une conservation."""
    if not conservation:
        return None

    conservation = normalize_value(conservation)
    return CONSERVATION_MAPPING.get(conservation, conservation)


def normalize_trim(trim: Optional[str]) -> Optional[str]:
    """Normalise un trim."""
    if not trim:
        return None

    trim = normalize_value(trim)
    return TRIM_MAPPING.get(trim, trim)


def extract_preparation_states_from_name(product_name: str) -> list[str]:
    """
    Extrait les états de préparation du nom de produit.

    Retourne uniquement les états destinés au champ decoupe (Vidé, Entier, Gratté).
    Exclut les vrais états (VIVANT, CUIT) qui restent dans le champ etat.

    Args:
        product_name: Nom du produit à analyser

    Returns:
        Liste des états normalisés dans l'ordre d'apparition (ex: ["Vidé", "Gratté"])

    Examples:
        >>> extract_preparation_states_from_name("DORADE VIDÉ GRATTÉ")
        ['Vidé', 'Gratté']
        >>> extract_preparation_states_from_name("SOLE ENTIÈRE VIVANTE")
        ['Entier']
        >>> extract_preparation_states_from_name("TURBOT NON VIDÉ")
        ['Non vidé']
    """
    if not product_name:
        return []

    name_upper = product_name.upper()

    # Trouver tous les matches avec leur position
    matches = []
    for pattern, normalized in PREPARATION_STATE_PATTERNS:
        for match in re.finditer(pattern, name_upper):
            matches.append((match.start(), match.end(), normalized))

    # Trier par position d'apparition
    matches.sort(key=lambda x: x[0])

    # Éliminer les chevauchements (garder le premier match en cas de conflit)
    # et dédupliquer
    found_states = []
    seen_normalized = set()
    covered_ranges = []

    for start, end, normalized in matches:
        # Vérifier si ce match chevauche un précédent
        overlaps = any(start < prev_end and end > prev_start for prev_start, prev_end in covered_ranges)

        if not overlaps and normalized not in seen_normalized:
            found_states.append(normalized)
            seen_normalized.add(normalized)
            covered_ranges.append((start, end))

    return found_states


def combine_decoupe_with_prep_states(
    decoupe: Optional[str],
    prep_states: list[str]
) -> Optional[str]:
    """
    Combine découpe physique et états de préparation.

    Format: "DECOUPE_PHYSIQUE, État 1, État 2"
    - Découpes physiques en MAJUSCULES
    - États de préparation en Title Case
    - Déduplication (insensible à la casse)

    Args:
        decoupe: Découpe physique (ex: "FILET", "DOS")
        prep_states: Liste des états de préparation

    Returns:
        Chaîne combinée ou None si tout est vide

    Examples:
        >>> combine_decoupe_with_prep_states("FILET", ["Vidé", "Gratté"])
        "FILET, Vidé, Gratté"
        >>> combine_decoupe_with_prep_states(None, ["Entier", "Non vidé"])
        "Entier, Non vidé"
        >>> combine_decoupe_with_prep_states("DOS", [])
        "DOS"
    """
    parts = []
    seen_lower = set()

    # Ajouter la découpe physique en premier (si présente)
    if decoupe:
        decoupe_clean = decoupe.strip()
        if decoupe_clean and decoupe_clean.lower() not in seen_lower:
            parts.append(decoupe_clean)
            seen_lower.add(decoupe_clean.lower())

    # Ajouter les états de préparation (déduplication)
    for state in prep_states:
        state_clean = state.strip()
        if state_clean and state_clean.lower() not in seen_lower:
            parts.append(state_clean)
            seen_lower.add(state_clean.lower())

    return ", ".join(parts) if parts else None


# =============================================================================
# FONCTIONS SPÉCIFIQUES DEMARNE
# =============================================================================

def _normalize_filet_species(species_raw: str) -> str:
    """
    Normalise une espèce extraite depuis une catégorie/variante FILET.

    Args:
        species_raw: Espèce brute extraite par regex

    Returns:
        Espèce normalisée
    """
    species_upper = remove_accents(species_raw.upper().strip())

    # Correspondance exacte
    if species_upper in FILET_SPECIES_NORMALIZE:
        return FILET_SPECIES_NORMALIZE[species_upper]

    # Correspondance par préfixe (ex: "MERLU A" → MERLU)
    for key, val in FILET_SPECIES_NORMALIZE.items():
        if species_upper.startswith(key):
            return val

    return species_upper


def extract_species_from_filet(categorie: str, variante: Optional[str] = None) -> Optional[str]:
    """
    Extrait l'espèce depuis une catégorie FILET et/ou sa variante.

    Logique:
    1. Si catégorie générique ("FILETS POISSON BLANC/BLEU", "POISSONS FILETS")
       → chercher l'espèce dans la variante
    2. Si catégorie spécifique ("FILET DE TRUITE", "FILETS D'ANCHOIS")
       → extraire l'espèce depuis la catégorie elle-même

    Args:
        categorie: Catégorie Demarne brute (ex: "FILETS POISSON BLANC")
        variante: Variante Demarne brute (ex: "Filet de merlu")

    Returns:
        Espèce normalisée ou None si non trouvée
    """
    cat_upper = remove_accents(categorie.upper()) if categorie else ''
    var_upper = remove_accents(variante.upper()) if variante else ''

    # 1. Catégorie générique → chercher dans variante
    if re.search(FILET_CAT_GENERIC_PATTERN, cat_upper):
        if variante:
            # Pattern principal: "Filet (de|d') {espèce}"
            match = re.search(FILET_VAR_SPECIES_PATTERN, var_upper)
            if match:
                return _normalize_filet_species(match.group(1))

            # Pattern alternatif: "Aile de Raie", "Pavé de Morue"
            alt_match = re.search(FILET_VAR_ALT_PATTERN, var_upper)
            if alt_match:
                return _normalize_filet_species(alt_match.group(1))

        return None  # Catégorie générique sans espèce trouvée dans variante

    # 2. Catégorie spécifique: "FILET(S) DE|D' {ESPECE}"
    match = re.search(FILET_CAT_SPECIES_PATTERN, cat_upper)
    if match:
        return _normalize_filet_species(match.group(1))

    return None


def extract_species_from_variante(variante: Optional[str]) -> tuple:
    """
    Extrait l'espèce et la découpe depuis une variante Demarne.

    Cas 1: Variante avec découpe → "Dos de cabillaud" → (CABILLAUD, DOS)
    Cas 2: Variante simple → "Sole" → (SOLE, None)

    Args:
        variante: Variante Demarne brute

    Returns:
        tuple (espèce, découpe) ou (None, None) si non trouvé
    """
    if not variante:
        return None, None

    var_upper = remove_accents(variante.upper().strip())

    # 1. Chercher pattern "découpe + espèce"
    for pattern, decoupe in VARIANTE_DECOUPE_PATTERNS:
        match = re.search(pattern, var_upper, re.IGNORECASE)
        if match:
            species_raw = match.group(1).strip()
            # Nettoyer suffixes courants (S, A, S/P, MSC, VDK, etc.)
            species_clean = re.sub(
                r'\s+(S|A|S/P|MSC|VDK|LIGNE|VIDE|VIDÉ|A/P|BLANC)$',
                '',
                species_raw,
                flags=re.IGNORECASE
            )
            species = _normalize_filet_species(species_clean)
            return species, decoupe

    # 2. Variante simple = espèce directe
    # Nettoyer la variante des suffixes courants
    species_raw = re.sub(
        r'\s+(S|A|VDK|LIGNE|VIDE|VIDÉ|gros|rouge)$',
        '',
        var_upper,
        flags=re.IGNORECASE
    )
    species = _normalize_filet_species(species_raw.strip())

    # Vérifier si c'est une espèce connue dans le mapping
    if species in FILET_SPECIES_NORMALIZE.values():
        return species, None

    # Vérifier avec les patterns d'espèces DEMARNE
    for pattern, sp in DEMARNE_SPECIES_PATTERNS:
        if re.search(pattern, species, re.IGNORECASE):
            return sp, None

    # Retourner quand même la variante nettoyée comme espèce
    return species, None


def normalize_demarne_categorie(
    categorie: Optional[str],
    product_name: Optional[str] = None,
    variante: Optional[str] = None
) -> dict:
    """
    Normalise une catégorie Demarne en extrayant:
    - categorie: Espèce pure
    - type_production: SAUVAGE ou ELEVAGE
    - qualite: SUP, PREMIUM, LABEL ROUGE
    - etat: ENTIER, VIDE, CUIT, etc.
    - origine_from_categorie: Origine extraite de la catégorie
    - decoupe_from_categorie: Découpe extraite si catégorie FILET

    Args:
        categorie: Catégorie Demarne brute (ex: "SAUMON SUPÉRIEUR NORVÈGE")
        product_name: Nom du produit pour contexte
        variante: Variante pour extraction d'espèce dans les catégories FILET

    Returns:
        dict avec les champs extraits
    """
    result = {
        "categorie": None,
        "type_production": None,
        "qualite": None,
        "etat": None,
        "origine_from_categorie": None,
        "decoupe_from_categorie": None,
    }

    if not categorie:
        return result

    cat_upper = remove_accents(categorie.upper().strip())

    # 1. Cas spécial: catégories FILET
    # Détecter si la catégorie contient FILET et extraire l'espèce
    if 'FILET' in cat_upper:
        species = extract_species_from_filet(categorie, variante)
        if species:
            result["categorie"] = species
            result["decoupe_from_categorie"] = "FILET"
        else:
            # Espèce non trouvée, garder la catégorie originale normalisée
            # mais marquer quand même la découpe comme FILET
            result["categorie"] = cat_upper
            result["decoupe_from_categorie"] = "FILET"

    # 2. Cas spécial: catégories génériques (DOS, AUTRES POISSONS, POISSON PLAT, etc.)
    # L'espèce est dans la variante, pas dans la catégorie
    elif cat_upper in DEMARNE_GENERIC_CATEGORIES:
        species, decoupe = extract_species_from_variante(variante)
        if species:
            result["categorie"] = species
            if decoupe:
                result["decoupe_from_categorie"] = decoupe
        else:
            # Fallback: garder la catégorie originale
            result["categorie"] = cat_upper

    else:
        # 3. Extraction standard via les patterns DEMARNE_SPECIES_PATTERNS
        for pattern, species in DEMARNE_SPECIES_PATTERNS:
            if re.search(pattern, cat_upper, re.IGNORECASE):
                result["categorie"] = species
                break

        # Si pas d'espèce trouvée, garder la catégorie originale
        if not result["categorie"]:
            result["categorie"] = cat_upper

    # 2. Extraire le type de production
    for pattern, type_prod in DEMARNE_TYPE_PRODUCTION_PATTERNS:
        if re.search(pattern, cat_upper, re.IGNORECASE):
            result["type_production"] = type_prod
            break

    # 3. Extraire la qualité
    for pattern, qualite in DEMARNE_QUALITE_PATTERNS:
        if re.search(pattern, cat_upper, re.IGNORECASE):
            result["qualite"] = qualite
            break

    # 4. Extraire l'état
    for pattern, etat in DEMARNE_ETAT_PATTERNS:
        if re.search(pattern, cat_upper, re.IGNORECASE):
            result["etat"] = etat
            break

    # 5. Extraire l'origine depuis la catégorie
    for pattern, origine in DEMARNE_ORIGINE_PATTERNS:
        if re.search(pattern, cat_upper, re.IGNORECASE):
            result["origine_from_categorie"] = origine
            break

    # 6. Affiner l'espèce avec la variante si elle contient des précisions
    # Ex: categorie="DORADE SAUVAGE" → espece="DORADE", mais variante="Dorade Grise" → espece="DORADE GRISE"
    if variante and result["categorie"]:
        var_upper = remove_accents(variante.upper().strip())

        # Si l'espèce est DORADE et la variante contient GRISE → DORADE GRISE
        if result["categorie"] == "DORADE" and "GRISE" in var_upper:
            result["categorie"] = "DORADE GRISE"

    return result


def normalize_demarne_variante(variante: Optional[str]) -> dict:
    """
    Normalise une variante Demarne en extrayant:
    - decoupe: FILET, DOS, QUEUE, etc.
    - etat: ENTIER, VIVANT, CUIT, etc.

    Args:
        variante: Variante Demarne brute

    Returns:
        dict avec 'decoupe' et 'etat'
    """
    result = {"decoupe": None, "etat": None}

    if not variante:
        return result

    var_upper = remove_accents(variante.upper().strip())

    # 1. Extraire la découpe
    for pattern, decoupe in DEMARNE_DECOUPE_PATTERNS:
        if re.search(pattern, var_upper, re.IGNORECASE):
            result["decoupe"] = decoupe
            break

    # 2. Extraire l'état
    for pattern, etat in DEMARNE_ETAT_PATTERNS:
        if re.search(pattern, var_upper, re.IGNORECASE):
            result["etat"] = etat
            break

    return result


def normalize_demarne_label(label: Optional[str]) -> dict:
    """
    Normalise un label Demarne en extrayant:
    - label: MSC, BIO, ASC, LABEL ROUGE, IGP, AOP
    - trim: TRIM_B, TRIM_D, TRIM_E

    Args:
        label: Label Demarne brut

    Returns:
        dict avec 'label' et 'trim'
    """
    result = {"label": None, "trim": None}

    if not label:
        return result

    label_upper = remove_accents(label.upper().strip())

    # 1. Extraire les labels officiels
    labels_found = []
    for official_label in DEMARNE_LABELS:
        if official_label in label_upper:
            labels_found.append(official_label)

    if labels_found:
        result["label"] = ", ".join(labels_found)

    # 2. Extraire le trim
    trim_match = re.search(r'TRIM\s*([BCDE])', label_upper)
    if trim_match:
        result["trim"] = f"TRIM_{trim_match.group(1)}"

    return result


def clean_demarne_origine(origine: Optional[str]) -> Optional[str]:
    """
    Nettoie une origine Demarne:
    - Filtre les poids erronés (ex: "200 grs", "1 kg")
    - Normalise les variations orthographiques

    Args:
        origine: Origine Demarne brute

    Returns:
        Origine nettoyée ou None si c'est un poids
    """
    if not origine:
        return None

    origine_str = str(origine).strip()

    # Filtrer les poids (patterns: "X kg", "X grs", "Xgrs")
    if re.match(r'^\d+\s*(kg|grs?|g)\s*$', origine_str, re.IGNORECASE):
        return None

    # Normaliser
    origine_upper = remove_accents(origine_str.upper())

    # Appliquer le mapping de normalisation
    if origine_upper in DEMARNE_ORIGINE_MAPPING:
        return DEMARNE_ORIGINE_MAPPING[origine_upper]

    return origine_upper


# =============================================================================
# EXTRACTION D'ESPÈCE
# =============================================================================

SPECIES_PATTERNS = [
    # Patterns triés du plus spécifique au plus générique
    (r'\bROUGET\s*BARBET\b', 'ROUGET BARBET'),
    (r'\bROUGET\b', 'ROUGET'),
    (r'\bSAINT\s*PIERRE\b', 'SAINT PIERRE'),
    (r'\bST\s*PIERRE\b', 'SAINT PIERRE'),
    (r'\bLIEU\s*JAUNE\b', 'LIEU JAUNE'),
    (r'\bLIEU\s*NOIR\b', 'LIEU NOIR'),
    (r'\bDORADE\s*PAGRE\b', 'DORADE PAGRE'),
    (r'\bCOQUILLE\s*ST\s*JACQUES\b', 'COQUILLE ST JACQUES'),
    (r'\bNOIX\s*ST\s*JACQUES\b', 'NOIX ST JACQUES'),
    # Espèces simples
    (r'\bBAR\b', 'BAR'),
    (r'\bBARBUE\b', 'BARBUE'),
    (r'\bTURBOT\b', 'TURBOT'),
    (r'\bSOLE\b', 'SOLE'),
    (r'\bPAGEOT\b', 'PAGEOT'),  # Avant CABILLAUD pour éviter capture
    (r'\bCABILLAUD\b', 'CABILLAUD'),
    (r'\bMERLU\b', 'MERLU'),
    (r'\bMERLAN\b', 'MERLAN'),
    (r'\bLOTTE\b', 'LOTTE'),
    (r'\bDORADE\s+GRISE\b', 'DORADE GRISE'),
    (r'\bDORADE\b', 'DORADE'),
    (r'\bRAIE\b', 'RAIE'),
    (r'\bSAUMON\b', 'SAUMON'),
    (r'\bTHON\b', 'THON'),
    (r'\bMAIGRE\b', 'MAIGRE'),
    (r'\bCONGRE\b', 'CONGRE'),
    (r'\bGRONDIN\b', 'GRONDIN'),
    (r'\bPAGRE\b', 'PAGRE'),
    (r'\bENCORNET\b', 'ENCORNET'),
    (r'\bPOULPE\b', 'POULPE'),
    (r'\bSEICHE\b', 'SEICHE'),
    (r'\bHOMARD\b', 'HOMARD'),
    (r'\bLANGOUSTE\b', 'LANGOUSTE'),
    (r'\bLANGOUSTINE\b', 'LANGOUSTINE'),
    (r'\bCREVETTE\b', 'CREVETTES'),
    (r'\bLIMANDE\b', 'LIMANDE'),
    (r'\bCARRELET\b', 'CARRELET'),
    # Espèces ajoutées pour VVQM (section FILETS)
    (r'\bESPADON\b', 'ESPADON'),
    (r'\bMOSTELLE\b', 'MOSTELLE'),
    (r'\bJULIENNE\b', 'JULIENNE'),
    (r'\bGRENADIER\b', 'GRENADIER'),
    (r'\bELINGUE\b', 'ELINGUE'),
    (r'\bSABRE\b', 'SABRE'),
    (r'\bBROSME\b', 'BROSME'),
    # Espèces ajoutées pour Laurent Daniel
    (r'\bTACAUD\b', 'TACAUD'),
    (r'\bLINGUE\b', 'LINGUE'),
    (r'\bMORUETTE\b', 'MORUETTE'),
    (r'\bANON\b', 'ANON'),
]


def extract_species_from_name(product_name: str) -> Optional[str]:
    """
    Extrait l'espèce depuis un nom de produit.
    Utilisé notamment pour les catégories FILET.
    """
    if not product_name:
        return None

    product_upper = product_name.upper()

    for pattern, species in SPECIES_PATTERNS:
        if re.search(pattern, product_upper):
            return species

    return None


def determine_filet_meaning(text: str) -> dict:
    """
    Détermine si FILET dans un texte représente une découpe ou une méthode de pêche
    basé sur sa position relative à l'espèce.

    Règles:
    - FILET AVANT espèce (ex: "Filet de Bar") → découpe
    - FILET APRÈS espèce (ex: "Bar de Filet", "Bar filet") → méthode de pêche

    Args:
        text: Nom de produit ou catégorie à analyser

    Returns:
        dict avec 'is_decoupe', 'is_methode_peche', 'species'
    """
    result = {"is_decoupe": False, "is_methode_peche": False, "species": None}

    if not text:
        return result

    text_upper = text.upper()

    # Chercher la position de FILET/FILETS
    filet_match = re.search(r'\bFILETS?\b', text_upper)
    if not filet_match:
        return result

    filet_pos = filet_match.start()

    # Chercher la position de l'espèce
    species_pos = None
    species_found = None
    for pattern, species in SPECIES_PATTERNS:
        match = re.search(pattern, text_upper)
        if match:
            species_pos = match.start()
            species_found = species
            break

    result["species"] = species_found

    # Si pas d'espèce trouvée
    if species_pos is None:
        # Pattern "FILET DE..." sans espèce reconnue → découpe par défaut
        if re.search(r'\bFILETS?\s+(?:DE\s+|D\')', text_upper):
            result["is_decoupe"] = True
        else:
            # FILET seul ou "FILET DE POISSONS" générique → découpe
            result["is_decoupe"] = True
        return result

    # Comparer les positions
    if filet_pos < species_pos:
        # FILET avant espèce → découpe (ex: "Filet de Bar")
        result["is_decoupe"] = True
    else:
        # FILET après espèce → méthode de pêche (ex: "Bar de Filet", "Bar filet")
        result["is_methode_peche"] = True

    return result


# =============================================================================
# FONCTION PRINCIPALE D'HARMONISATION
# =============================================================================


# MAPPING DES CLÉS STRUCTURELLES (CamelCase -> snake_case)
STRUCTURAL_KEYS_MAPPING = {
    "Date": "date",
    "Vendor": "vendor",
    "Code_Provider": "code_provider",
    "Code": "code_provider",  # Demarne uses "Code" instead of "Code_Provider"
    "ProductName": "product_name",
    "Prix": "prix",
    "Tarif": "prix",  # Alias
    "Colisage": "colisage",
    "Unite_Facturee": "unite_facturee",
    "Infos_Brutes": "infos_brutes",
}

def harmonize_product(product: dict, vendor: str = None) -> dict:
    """
    Harmonise un produit selon les règles définies.

    Args:
        product: Dictionnaire produit (sortie brute d'un parseur)
        vendor: Nom du fournisseur (pour règles spécifiques)

    Returns:
        Dictionnaire produit harmonisé avec tous les champs normalisés
    """
    result = product.copy()

    # 1. Harmonisation des attributs produits (spécifique vs générique)
    if vendor and vendor.lower() == "demarne":
        result = _harmonize_demarne_product(result)
    else:
        # Harmonisation générique
        
        # --- Categorie ---
        cat_result = normalize_categorie(
            result.get("Categorie") or result.get("categorie"),
            result.get("ProductName") or result.get("product_name")
        )
        result["categorie"] = cat_result["categorie"]

        # Si on a extrait une découpe depuis la catégorie, l'ajouter
        if cat_result.get("decoupe_from_categorie"):
            if not result.get("decoupe") and not result.get("Decoupe"):
                result["decoupe"] = cat_result["decoupe_from_categorie"]

        # Si on a extrait une méthode de pêche depuis la catégorie (ex: "BAR FILET")
        if cat_result.get("methode_peche_from_categorie"):
            if not result.get("methode_peche") and not result.get("Methode_Peche"):
                result["methode_peche"] = cat_result["methode_peche_from_categorie"]
            # Si FILET est methode_peche, ne pas le garder comme decoupe
            # (le parseur peut avoir extrait FILET comme decoupe par erreur)
            if cat_result["methode_peche_from_categorie"] == "FILET":
                if result.get("Decoupe") == "FILET":
                    result["Decoupe"] = None
                if result.get("decoupe") == "FILET":
                    result["decoupe"] = None

        # --- Methode_Peche ---
        methode_result = normalize_methode_peche(
            result.get("Methode_Peche") or result.get("methode_peche")
        )
        # Ne pas écraser si déjà défini par la catégorie
        if not result.get("methode_peche"):
            result["methode_peche"] = methode_result["methode_peche"]

        # Champs extraits
        if methode_result["type_production"]:
            result["type_production"] = methode_result["type_production"]

        # --- Etat ---
        etat_result = normalize_etat(
            result.get("Etat") or result.get("etat")
        )
        result["etat"] = etat_result["etat"]
        if etat_result["couleur"]:
            result["couleur"] = etat_result["couleur"]

        # --- Origine ---
        origine_result = normalize_origine(
            result.get("Origine") or result.get("origine")
        )
        result["origine"] = origine_result["origine"]
        # Ne pas écraser type_production si déjà défini
        if origine_result["type_production"] and not result.get("type_production"):
            result["type_production"] = origine_result["type_production"]

        # --- Qualite ---
        result["qualite"] = normalize_qualite(
            result.get("Qualite") or result.get("qualite")
        )

        # --- Vérifier FILET dans ProductName (si pas déjà géré par la catégorie) ---
        # Si le ProductName contient "espèce + FILET" (ex: "Bar filet"), c'est une methode_peche
        if not cat_result.get("methode_peche_from_categorie"):
            product_name = result.get("ProductName") or result.get("product_name") or ""
            if product_name and "FILET" in product_name.upper():
                filet_meaning = determine_filet_meaning(product_name)
                if filet_meaning["is_methode_peche"]:
                    if not result.get("methode_peche"):
                        result["methode_peche"] = "FILET"
                    # Supprimer FILET de decoupe si le parseur l'a extrait
                    if result.get("Decoupe") == "FILET":
                        result["Decoupe"] = None
                    if result.get("decoupe") == "FILET":
                        result["decoupe"] = None

        # --- Decoupe ---
        # Ne pas écraser si déjà défini par normalize_categorie
        if not result.get("decoupe"):
            result["decoupe"] = normalize_decoupe(
                result.get("Decoupe") or result.get("decoupe")
            )

        # Détecter le mot-clé "Decoupe"/"Découpe" dans le nom du produit
        if not result.get("decoupe"):
            product_name = result.get("ProductName") or result.get("product_name") or ""
            if product_name and re.search(r'\bD[EÉ]COUPE\b', product_name.upper()):
                result["decoupe"] = "DECOUPE"

        # --- Calibre ---
        result["calibre"] = normalize_calibre(
            result.get("Calibre") or result.get("calibre")
        )

        # --- Conservation ---
        result["conservation"] = normalize_conservation(
            result.get("Conservation") or result.get("conservation")
        )

        # --- Trim ---
        result["trim"] = normalize_trim(
            result.get("Trim") or result.get("trim")
        )

        # --- Extraction des états de préparation et combinaison avec decoupe ---
        product_name_for_prep = result.get("ProductName") or result.get("product_name") or ""
        prep_states = extract_preparation_states_from_name(product_name_for_prep)

        # Combiner avec la découpe physique existante
        result["decoupe"] = combine_decoupe_with_prep_states(
            result.get("decoupe"),
            prep_states
        )

        # --- Nettoyage: supprimer les anciennes clés en CamelCase (attributs produits) ---
        old_keys = [
            "Categorie", "Methode_Peche", "Qualite", "Decoupe", "Etat",
            "Origine", "Calibre", "Conservation", "Trim"
        ]
        for key in old_keys:
            if key in result and key.lower() in result:
                del result[key]

    # 2. Harmonisation structurelle (renommage des clés principales)
    # Appliquer le mapping CamelCase -> snake_case
    for old_key, new_key in STRUCTURAL_KEYS_MAPPING.items():
        if old_key in result:
            if new_key not in result:
                result[new_key] = result[old_key]
            # Supprimer l'ancienne clé sauf si c'est la même
            if old_key != new_key:
                del result[old_key]
    
    # S'assurer que code_provider est toujours une string (Demarne a des codes numériques)
    if "code_provider" in result and result["code_provider"] is not None:
        result["code_provider"] = str(result["code_provider"])

    return result


def _harmonize_demarne_product(product: dict) -> dict:
    """
    Harmonise un produit Demarne avec ses règles spécifiques.

    Demarne a une structure différente:
    - Categorie contient espèce + type_production + origine + qualite
    - Variante contient découpe + état
    - Label contient certifications + trim

    Args:
        product: Dictionnaire produit Demarne

    Returns:
        Dictionnaire produit harmonisé
    """
    result = product.copy()

    # Récupérer les valeurs brutes
    categorie_raw = result.get("Categorie") or result.get("categorie")
    variante_raw = result.get("Variante") or result.get("variante")
    product_name_raw = result.get("ProductName") or result.get("product_name")

    # --- 1. Traiter la Categorie Demarne ---
    # Passer la variante pour extraction d'espèce dans les catégories FILET
    cat_result = normalize_demarne_categorie(
        categorie_raw,
        product_name_raw,
        variante_raw
    )
    result["categorie"] = cat_result["categorie"]

    # Extraire type_production, qualite, etat, origine depuis la catégorie
    if cat_result["type_production"]:
        result["type_production"] = cat_result["type_production"]
    if cat_result["qualite"]:
        result["qualite"] = cat_result["qualite"]
    if cat_result["etat"]:
        result["etat"] = cat_result["etat"]

    # Si découpe extraite depuis catégorie FILET, la définir
    if cat_result.get("decoupe_from_categorie"):
        result["decoupe"] = cat_result["decoupe_from_categorie"]

    # --- 2. Traiter la Variante Demarne ---
    var_result = normalize_demarne_variante(variante_raw)
    # Découpe depuis variante: ne pas écraser si déjà défini depuis catégorie FILET
    if var_result["decoupe"] and not result.get("decoupe"):
        result["decoupe"] = var_result["decoupe"]
    if var_result["etat"] and not result.get("etat"):
        result["etat"] = var_result["etat"]

    # --- 3. Traiter le Label Demarne ---
    label_result = normalize_demarne_label(
        result.get("Label") or result.get("label")
    )
    if label_result["label"]:
        result["label"] = label_result["label"]
    if label_result["trim"]:
        result["trim"] = label_result["trim"]

    # --- 4. Traiter l'Origine Demarne ---
    # D'abord nettoyer la colonne Origine (enlever les poids)
    origine_cleaned = clean_demarne_origine(
        result.get("Origine") or result.get("origine")
    )

    # Si l'origine nettoyée est vide mais on a extrait une origine de la catégorie
    if not origine_cleaned and cat_result.get("origine_from_categorie"):
        result["origine"] = cat_result["origine_from_categorie"]
    elif origine_cleaned:
        result["origine"] = origine_cleaned
    else:
        result["origine"] = None

    # --- 5. Traiter la Methode_Peche ---
    methode_result = normalize_methode_peche(
        result.get("Methode_Peche") or result.get("methode_peche")
    )
    result["methode_peche"] = methode_result["methode_peche"]

    # Ne pas écraser type_production si déjà défini
    if methode_result["type_production"] and not result.get("type_production"):
        result["type_production"] = methode_result["type_production"]

    # --- 6. Traiter le Calibre ---
    result["calibre"] = normalize_calibre(
        result.get("Calibre") or result.get("calibre")
    )

    # --- 7. Extraction des états de préparation et combinaison avec decoupe ---
    product_name_for_prep = result.get("ProductName") or result.get("product_name") or ""
    prep_states = extract_preparation_states_from_name(product_name_for_prep)

    # Combiner avec la découpe physique existante
    result["decoupe"] = combine_decoupe_with_prep_states(
        result.get("decoupe"),
        prep_states
    )

    # --- 8. Nettoyage des anciennes clés ---
    old_keys = [
        "Categorie", "Variante", "Methode_Peche", "Label", "Calibre", "Origine",
        "Qualite", "Decoupe", "Etat", "Conservation", "Trim"
    ]
    for key in old_keys:
        lower_key = key.lower()
        if key in result and lower_key in result:
            del result[key]

    return result


def harmonize_products(products: list[dict], vendor: str = None) -> list[dict]:
    """
    Harmonise une liste de produits.

    Args:
        products: Liste de dictionnaires produits
        vendor: Nom du fournisseur

    Returns:
        Liste de dictionnaires produits harmonisés
    """
    return [harmonize_product(p, vendor) for p in products]
