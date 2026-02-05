"""
Parser pour les PDF des Viviers d'Audierne.

Structure du document:
- Header répété sur chaque page (logo, adresse, date)
- 2 colonnes de produits par page
- Sections en majuscules (LANGOUSTE, HOMARD, etc.)
- Produits: NOM.......... PRIX

Attributs extraits:
- Categorie: Section du PDF (LANGOUSTE, HOMARD, BAR, etc.)
- Methode_Peche: LIGNE, CHALUT, PB
- Qualite: PREMIUM, SUP, EXTRA, BIO
- Decoupe: FILET, DOS, DARNE, PAVE, COEUR, QUEUE, AILE, CHAIR
- Etat: DECONGELE, PASTEURISE, VIVANT, CUIT, SPECIALES
- Origine: DANEMARK, ATLANTIQUE, ECOSSE, NORVEGE, IRLANDE, FRANCE, CANCALE, AUDIERNE, AQUACULTURE
- Calibre: T-series (T1, T2...), plages (1/2, 500/600), N° (huîtres)
- Trim: TRIM C, TRIM D, TRIM E (spécifique saumon)

Harmonisation:
- Ce parseur supporte l'harmonisation via services/harmonize.py
- Utiliser parse_audierne(file_bytes, harmonize=True) pour obtenir des attributs normalisés
- Voir docs/harmonisation_attributs.md pour les règles de normalisation
"""
import fitz  # PyMuPDF
import re
import unicodedata
from datetime import datetime
from io import BytesIO
from typing import Optional

from parsers.utils import refine_generic_category

# Catégories génériques à affiner pour Audierne
AUDIERNE_GENERIC_CATEGORIES = {
    'DIVERS', 'DIVERS POISSONS',
    'COQUILLAGES',
    'CRUSTACES',
    'CRUSTACES CUITS PAST',
    'POISSONS BLEUS',
    'FILET DE POISSONS',
    'SAUMONS',
}

# Import conditionnel pour éviter les erreurs si harmonize.py n'existe pas encore
try:
    from services.harmonize import harmonize_products
    HARMONIZE_AVAILABLE = True
except ImportError:
    HARMONIZE_AVAILABLE = False


def normalize_code(text: str) -> str:
    """
    Normalise un texte pour créer un Code_Provider.
    - Supprime les accents
    - Convertit en minuscules
    - Remplace espaces et caractères spéciaux par des underscores
    - Supprime les underscores multiples
    """
    # Supprimer les accents
    text = unicodedata.normalize('NFD', text)
    text = ''.join(c for c in text if unicodedata.category(c) != 'Mn')
    # Minuscules
    text = text.lower()
    # Remplacer caractères non alphanumériques par underscore
    text = re.sub(r'[^a-z0-9]+', '_', text)
    # Supprimer underscores en début/fin et multiples
    text = re.sub(r'_+', '_', text).strip('_')
    return text


def extract_date_from_text(text: str) -> Optional[str]:
    """
    Extrait la date du texte au format YYYY-MM-DD.
    Cherche le pattern DD/MM/YYYY.
    """
    match = re.search(r'(\d{2})/(\d{2})/(\d{4})', text)
    if match:
        day, month, year = match.groups()
        return f"{year}-{month}-{day}"
    return None


def is_section_header(text: str) -> bool:
    """
    Détermine si une ligne est un header de section.
    Les sections sont en majuscules, sans prix, et souvent des noms de catégories.

    Exemples de sections valides:
    - LANGOUSTE
    - TOURTEAUX - ARAIGNEES
    - FILET DE POISSONS

    Exemples de NON-sections:
    - HOMARD EUROPEEN 4/600 (contient un calibre)
    - LANGOUSTE ROSE 60,60 (contient un prix)
    - PREMIUM (mot isolé, continuation de ligne)
    """
    text = text.strip()
    if not text:
        return False

    # Ignorer les lignes avec des prix
    if re.search(r'\d+,\d{2}\s*$', text):
        return False

    # Ignorer les lignes qui commencent par des mots de header de page
    # Note: Utiliser startswith() pour éviter les faux positifs (ex: "PAGEOT" ne doit pas matcher "page")
    header_words = ['bonjour', 'cours du', 'page ', 'opportunit', 'bonne journ',
                    'viviers', 'audierne', 'poissons / crustaces', 'demande de']
    text_lower = text.lower()
    if any(text_lower.startswith(word) for word in header_words):
        return False

    # Une section est généralement tout en majuscules
    if not text.isupper() or len(text) <= 2:
        return False

    # Les sections n'ont pas de points de remplissage
    if '..' in text:
        return False

    # Mots qui sont des continuations de ligne, pas des sections
    continuation_words = [
        'PREMIUM', 'PASTEURISE', 'BLANCHE', 'NOIRE', 'PB', 'LIGNE',
        'PELÉE', 'PELEE', 'NP', 'VAT', 'VDK', 'VIDE', 'TRIM',
        'ECOS', 'IRL', 'NORVEGE', 'ECOSSE', 'FRANCE', 'AP',
        'TRIM C', 'TRIM D', 'TRIM E',  # Trims saumon
        'SUP', 'EXTRA', 'BIO',  # Qualités
    ]
    if text in continuation_words:
        return False

    # Les sections ne contiennent généralement pas de patterns de calibre/poids
    # Ex: 4/600, 1/2 KG, T2, 500gr
    calibre_patterns = [
        r'\d+/\d+',          # 4/600, 1/2
        r'\d+\s*gr',         # 500gr
        r'\d+\s*kg',         # 2 KG
        r'\bT\d+',           # T2, T11
        r'\d+\.\d+',         # 1.5
        r'N°\d+',            # N°2
    ]
    for pattern in calibre_patterns:
        if re.search(pattern, text, re.IGNORECASE):
            return False

    # Liste des sections connues valides (pour validation)
    known_sections = [
        'LANGOUSTE', 'HOMARD', 'TOURTEAUX', 'ARAIGNEES', 'TOURTEAUX - ARAIGNEES',
        'CRUSTACES CUITS PAST', 'TURBOT', 'BARBUE', 'SOLE', 'LOTTE', 'MERLU',
        'POULPE', 'ENCORNET', 'BAR', 'LIEU JAUNE', 'SAINT PIERRE', 'ROUGET BARBET',
        'GRONDIN', 'VIEILLE', 'CONGRE', 'DORADE', 'DORADE GRISE', 'CABILLAUD', 'PAGEOT', 'MERLAN',
        'PAGRE', 'TACAUD', 'RAIE', 'THON', 'POISSONS BLEUS', 'DIVERS POISSONS',
        'FILET DE POISSONS', 'SAUMONS', 'FILET SAUMON', 'COQUILLAGES'
    ]

    # Si c'est une section connue, c'est valide
    if text in known_sections:
        return True

    # Sinon, vérifier que ça ressemble à une section (mots courts, pas trop de mots)
    words = text.split()
    if len(words) > 4:  # Une section a généralement 1-4 mots
        return False

    return True


def extract_product_and_price(text: str) -> tuple[Optional[str], Optional[float]]:
    """
    Extrait le nom du produit et le prix d'une ligne.
    Retourne (product_name, price) ou (None, None) si pas de match.
    """
    text = text.strip()
    if not text:
        return None, None

    # Pattern 1: Nom suivi de points et prix
    # Ex: "LANGOUSTE ROSE 4/600.............................. 60,60"
    match = re.match(r'^(.+?)\.{2,}\s*(\d{1,3},\d{2})\s*$', text)
    if match:
        product = match.group(1).strip()
        price_str = match.group(2).replace(',', '.')
        return product, float(price_str)

    # Pattern 2: Nom suivi directement du prix (sans points)
    # Ex: "ORMEAUX 39,10"
    match = re.match(r'^(.+?)\s+(\d{1,3},\d{2})\s*$', text)
    if match:
        product = match.group(1).strip()
        # Vérifier que le produit n'est pas juste des chiffres
        if not re.match(r'^[\d\s/,\.]+$', product):
            price_str = match.group(2).replace(',', '.')
            return product, float(price_str)

    return None, None


def parse_audierne_attributes(product_name: str, categorie: str = None) -> dict:
    """
    Extrait les attributs structurés depuis ProductName pour Audierne.
    Inspiré de parse_hennequin_attributes().

    Args:
        product_name: Nom complet du produit
        categorie: Catégorie du produit (optionnel)

    Returns:
        dict avec: Methode_Peche, Qualite, Decoupe, Etat, Origine, Calibre, Trim, Infos_Brutes
    """
    result = {
        "Methode_Peche": None,
        "Qualite": None,
        "Decoupe": None,
        "Etat": None,
        "Origine": None,
        "Calibre": None,
        "Trim": None,
        "Infos_Brutes": None,
    }

    if not product_name:
        return result

    text_upper = product_name.upper()
    infos_trouvees = []

    # --- Méthode de pêche ---
    methode_patterns = [
        (r'\bLIGNE\b', 'LIGNE'),
        (r'\bCHALUT\b', 'CHALUT'),
        (r'\bPB\b', 'PB'),  # Petit Bateau
    ]
    for pattern, method in methode_patterns:
        if re.search(pattern, text_upper):
            result["Methode_Peche"] = method
            infos_trouvees.append(f"Méthode:{method}")
            break

    # --- Qualité ---
    qualite_patterns = [
        (r'\bPREMIUM\b', 'PREMIUM'),
        (r'\bSUP\b', 'SUP'),
        (r'\bEXTRA\b', 'EXTRA'),
        (r'\bBIO\b', 'BIO'),
    ]
    for pattern, qualite in qualite_patterns:
        if re.search(pattern, text_upper):
            result["Qualite"] = qualite
            infos_trouvees.append(f"Qualité:{qualite}")
            break

    # --- Découpe ---
    decoupe_patterns = [
        (r'\bFILET\b', 'FILET'),
        (r'\bDOS\b', 'DOS'),
        (r'\bDARNE\b', 'DARNE'),
        (r'\bPAVE\b', 'PAVE'),
        (r'\bCOEUR\b', 'COEUR'),
        (r'\bQUEUE\b', 'QUEUE'),
        (r'\bAILE\b', 'AILE'),
        (r'\bCHAIR\b', 'CHAIR'),
    ]
    for pattern, decoupe in decoupe_patterns:
        if re.search(pattern, text_upper):
            result["Decoupe"] = decoupe
            infos_trouvees.append(f"Découpe:{decoupe}")
            break

    # --- État/Conservation ---
    etat_patterns = [
        (r'\bDECONGELE[ES]?\b', 'DECONGELE'),
        (r'\bPASTEURISE[ES]?\b', 'PASTEURISE'),
        (r'\bVIVANT[ES]?\b', 'VIVANT'),
        (r'\bCUIT[ES]?\b', 'CUIT'),
        (r'\bSPECIALES?\b', 'SPECIALES'),
    ]
    for pattern, etat in etat_patterns:
        if re.search(pattern, text_upper):
            result["Etat"] = etat
            infos_trouvees.append(f"État:{etat}")
            break

    # --- Origine ---
    origine_patterns = [
        (r'\bVDK\b', 'DANEMARK'),
        (r'\bVAT\b', 'ATLANTIQUE'),
        (r'\bECOS(?:SE)?\b', 'ECOSSE'),
        (r'\bNORVEGE\b', 'NORVEGE'),
        (r'\bIRLANDE\b', 'IRLANDE'),  # Full word - checked FIRST
        (r'\bIRL\b', 'IRLANDE'),      # Abbreviation - checked SECOND
        (r'\bFRANCE\b', 'FRANCE'),
        (r'\bCANCALE\b', 'CANCALE'),
        (r'\bVDA\b', 'AUDIERNE'),  # Viviers d'Audierne
        (r'\bAQ\b', 'AQUACULTURE'),
    ]
    origines_trouvees = []
    for pattern, origine in origine_patterns:
        if re.search(pattern, text_upper) and origine not in origines_trouvees:
            origines_trouvees.append(origine)
            infos_trouvees.append(f"Origine:{origine}")
    if origines_trouvees:
        result["Origine"] = ", ".join(origines_trouvees)

    # --- Trim (spécifique saumon) ---
    trim_match = re.search(r'\bTRIM\s*([A-E])\b', text_upper)
    if trim_match:
        trim_value = f"TRIM {trim_match.group(1)}"
        result["Trim"] = trim_value
        infos_trouvees.append(f"Trim:{trim_value}")

    # --- Calibre ---
    calibre_trouve = None

    # Pattern 1: Taille T avec plage optionnelle entre parenthèses
    # Ex: T2 (1/2 kg), T11 (2/4kg), T3 (0,500/1kg)
    match_t = re.search(r'\bT\s*(\d+)\s*(\([^)]+\))?', text_upper)
    if match_t:
        calibre_trouve = f"T{match_t.group(1)}"
        if match_t.group(2):
            calibre_trouve += f" {match_t.group(2)}"

    # Pattern 2: Plages numériques (1/2, 4/600, 1.5/2, 800/1.2)
    if not calibre_trouve:
        match_plage = re.search(r'\b(\d+(?:[,.]?\d*)?)\s*/\s*(\d+(?:[,.]?\d*)?(?:kg|gr)?|\+)', text_upper)
        if match_plage:
            calibre_trouve = f"{match_plage.group(1)}/{match_plage.group(2)}"

    # Pattern 3: Huîtres (N°2, N°3, N° 2)
    if not calibre_trouve:
        match_huitre = re.search(r'\bN°\s*(\d+)\b', text_upper)
        if match_huitre:
            calibre_trouve = f"N°{match_huitre.group(1)}"

    # Pattern 4: Poids simple (454gr, 500gr)
    if not calibre_trouve:
        match_poids = re.search(r'\b(\d+)\s*(gr|kg)\b', text_upper)
        if match_poids:
            calibre_trouve = f"{match_poids.group(1)}{match_poids.group(2).lower()}"

    if calibre_trouve:
        result["Calibre"] = calibre_trouve
        infos_trouvees.append(f"Calibre:{calibre_trouve}")

    # --- Infos brutes ---
    if infos_trouvees:
        result["Infos_Brutes"] = " | ".join(infos_trouvees)

    return result


def extract_audierne_data_from_pdf(file_bytes: bytes) -> list[dict]:
    """
    Extrait les données produits d'un PDF Viviers d'Audierne.

    Args:
        file_bytes: Contenu binaire du fichier PDF

    Returns:
        Liste de dictionnaires avec les champs:
        - Date, Vendor, keyDate, Code_Provider, Prix, ProductName, Categorie, IsOpportunite
    """
    doc = fitz.open(stream=BytesIO(file_bytes), filetype="pdf")

    all_products = []
    document_date = None
    # La section courante persiste à travers les colonnes et les pages
    # Ordre de lecture: P1C1 -> P1C2 -> P2C1 -> P2C2 -> ...
    current_section = None

    for page_num, page in enumerate(doc):
        page_width = page.rect.width
        mid_x = page_width / 2

        # Extraire le texte avec les coordonnées
        blocks = page.get_text("dict")["blocks"]

        # Collecter tous les spans individuellement avec leurs coordonnées et taille de police
        all_spans = []

        for block in blocks:
            if "lines" not in block:
                continue

            for line in block["lines"]:
                for span in line["spans"]:
                    text = span["text"].strip()
                    if text:
                        x = span["bbox"][0]  # x0
                        y = span["bbox"][1]  # y0
                        font_size = span["size"]  # Taille de police
                        all_spans.append({
                            "text": text,
                            "x": x,
                            "y": y,
                            "font_size": font_size,
                            "column": "left" if x < mid_x else "right"
                        })

        # Grouper les spans par ligne (coordonnée Y similaire, tolérance de 2 pixels)
        # et par colonne
        def group_spans_by_line(spans, y_tolerance=3):
            """Groupe les spans qui sont sur la même ligne visuelle."""
            if not spans:
                return []

            # Trier par Y puis par X
            sorted_spans = sorted(spans, key=lambda s: (s["y"], s["x"]))

            lines = []
            current_line = [sorted_spans[0]]
            current_y = sorted_spans[0]["y"]

            for span in sorted_spans[1:]:
                if abs(span["y"] - current_y) <= y_tolerance:
                    # Même ligne
                    current_line.append(span)
                else:
                    # Nouvelle ligne
                    lines.append(current_line)
                    current_line = [span]
                    current_y = span["y"]

            lines.append(current_line)
            return lines

        # Séparer par colonne
        left_spans = [s for s in all_spans if s["column"] == "left"]
        right_spans = [s for s in all_spans if s["column"] == "right"]

        # Grouper par ligne pour chaque colonne
        left_lines = group_spans_by_line(left_spans)
        right_lines = group_spans_by_line(right_spans)

        # Convertir les groupes de spans en lignes de texte
        def spans_to_lines(grouped_spans):
            """Convertit des groupes de spans en lignes avec texte combiné."""
            result = []
            for line_spans in grouped_spans:
                # Trier par X pour avoir le bon ordre
                sorted_by_x = sorted(line_spans, key=lambda s: s["x"])
                combined_text = " ".join(s["text"] for s in sorted_by_x)
                min_x = min(s["x"] for s in sorted_by_x)
                avg_y = sum(s["y"] for s in sorted_by_x) / len(sorted_by_x)
                # Prendre la taille de police max (le texte principal, pas les petits détails)
                max_font_size = max(s.get("font_size", 0) for s in sorted_by_x)
                result.append({
                    "text": combined_text,
                    "x": min_x,
                    "y": avg_y,
                    "font_size": max_font_size
                })
            return result

        lines_data_left = spans_to_lines(left_lines)
        lines_data_right = spans_to_lines(right_lines)

        # Extraire la date du document (une seule fois)
        if document_date is None:
            page_text = page.get_text()
            document_date = extract_date_from_text(page_text)

        # Seuil de taille de police pour distinguer sections (10.5) des produits (8.8)
        SECTION_FONT_SIZE_THRESHOLD = 9.5

        # Fonction pour ajouter un produit finalisé
        def add_product(product_name, price):
            code = f"AUD_{normalize_code(product_name)}"
            attrs = parse_audierne_attributes(product_name, current_section)
            all_products.append({
                "Date": document_date,
                "Vendor": "Audierne",
                "keyDate": f"{code}{document_date}" if document_date else code,
                "Code_Provider": code,
                "Prix": price,
                "ProductName": product_name,
                "Categorie": current_section,
                "IsOpportunite": False,
                # Attributs structurés
                "Methode_Peche": attrs["Methode_Peche"],
                "Qualite": attrs["Qualite"],
                "Decoupe": attrs["Decoupe"],
                "Etat": attrs["Etat"],
                "Origine": attrs["Origine"],
                "Calibre": attrs["Calibre"],
                "Trim": attrs["Trim"],
                "Infos_Brutes": attrs["Infos_Brutes"],
            })

        # Traiter chaque colonne (déjà triées par Y)
        # L'ordre est important: gauche puis droite, pour que la section persiste correctement
        for column_lines in [lines_data_left, lines_data_right]:
            # Produit en attente avec son prix (on attend de voir si la ligne suivante est une continuation)
            pending_product = None
            pending_price = None

            for line_info in column_lines:
                text = line_info["text"]
                font_size = line_info.get("font_size", 0)

                # Ignorer les headers de page
                if any(kw in text.lower() for kw in ['viviers', 'audierne', 'cours du',
                       'page ', 'bonjour', 'voici nos', 'poissons / crustaces',
                       'demande de pi', 'opportunit', 'bonne journ', '@', 'rue mole',
                       '+33', '.fr']):
                    continue

                # Détecter une section (taille de police >= 10 ET passe le test is_section_header)
                if font_size >= SECTION_FONT_SIZE_THRESHOLD and is_section_header(text):
                    # Avant de changer de section, finaliser le produit en attente
                    if pending_product and pending_price is not None:
                        add_product(pending_product, pending_price)
                    current_section = text.strip()
                    pending_product = None
                    pending_price = None
                    continue

                # Extraire produit et prix
                product_name, price = extract_product_and_price(text)

                if product_name and price is not None:
                    # On a trouvé un nouveau produit avec prix
                    # D'abord, finaliser le produit précédent en attente
                    if pending_product and pending_price is not None:
                        add_product(pending_product, pending_price)

                    # Mettre ce nouveau produit en attente (au cas où la ligne suivante serait une continuation)
                    pending_product = product_name
                    pending_price = price

                elif text.strip() and font_size < SECTION_FONT_SIZE_THRESHOLD:
                    # Ligne sans prix avec petite police = continuation du produit en attente
                    # Ex: "TRIM C", "PREMIUM", "PASTEURISE (pot)"
                    if pending_product:
                        # Ajouter cette continuation au produit en attente
                        pending_product = f"{pending_product} {text.strip()}"

            # Fin de la colonne : finaliser le dernier produit en attente
            if pending_product and pending_price is not None:
                add_product(pending_product, pending_price)
                pending_product = None
                pending_price = None

    doc.close()
    return all_products


def parse_audierne(file_bytes: bytes, harmonize: bool = False, **kwargs) -> list[dict]:
    """
    Point d'entrée principal du parser Audierne.

    Extrait les produits d'un PDF Viviers d'Audierne et optionnellement
    applique les règles d'harmonisation pour normaliser les attributs.

    Args:
        file_bytes: Contenu binaire du fichier PDF
        harmonize: Si True, applique l'harmonisation des attributs
                   (normalise les valeurs selon docs/harmonisation_attributs.md)
        **kwargs: Arguments supplémentaires (réservés pour extensions futures)

    Returns:
        Liste de dictionnaires produits avec les champs:
        - Date, Vendor, keyDate, Code_Provider, Prix, ProductName
        - Categorie, Methode_Peche, Qualite, Decoupe, Etat, Origine, Calibre, Trim
        - Infos_Brutes (concaténation des attributs extraits)

        Si harmonize=True, les clés sont en snake_case et les valeurs normalisées:
        - categorie, methode_peche, qualite, decoupe, etat, origine, calibre, trim
        - type_production (extrait de origine si AQUACULTURE)

    Example:
        >>> with open("cours_audierne.pdf", "rb") as f:
        ...     products = parse_audierne(f.read(), harmonize=True)
        >>> products[0]["categorie"]
        'SAINT PIERRE'  # Normalisé depuis 'ST PIERRE'
    """
    # Extraction des données brutes
    products = extract_audierne_data_from_pdf(file_bytes)

    # Affinage des catégories génériques vers espèces spécifiques
    for product in products:
        product["Categorie"] = refine_generic_category(
            product.get("Categorie"),
            product.get("ProductName"),
            AUDIERNE_GENERIC_CATEGORIES
        )

    # Application optionnelle de l'harmonisation
    if harmonize:
        if not HARMONIZE_AVAILABLE:
            raise ImportError(
                "Le module services.harmonize n'est pas disponible. "
                "Vérifiez que services/harmonize.py existe."
            )
        products = harmonize_products(products, vendor="Audierne")

    return products


# Alias pour compatibilité avec l'ancien nom
parse = parse_audierne
