"""
Parser pour les PDF VVQM (Viviers et Vrac Qualité Marée).

Structure du document:
- Sections de produits avec headers
- Prix en gras détectés automatiquement
- Calibres associés par proximité spatiale
- Catégories automatiques basées sur l'espèce

Attributs extraits:
- Categorie: Via mapping espèce → catégorie automatique (BAR, TURBOT, etc.)
- Methode_Peche: PB, LIGNE, IKEJIME (ou combinaison "LIGNE IKEJIME")
- Decoupe: DOS, FILET, JOUE, LONGE
- Etat: VIDÉ, CORAIL, BLANCHE, VIVANT, DÉC, ENTIÈRE
- Origine: ATLANTIQUE (VAT), DANEMARK (VDK)
- Calibre: Plages numériques (1/2, 500/600), format plus (500+)

Harmonisation:
- Ce parseur supporte l'harmonisation via services/harmonize.py
- Utiliser parse(file_bytes, harmonize=True) pour obtenir des attributs normalisés
- Voir docs/harmonisation_attributs.md pour les règles de normalisation

Spécificités:
- "LIGNE IKEJIME" est séparé en methode_peche=LIGNE + technique_abattage=IKEJIME
- Les accents sont normalisés (VIDÉ → VIDE, ENTIÈRE → ENTIER)
"""
import fitz
import pandas as pd
import re
import logging
from parsers.utils import sanitize_for_json, refine_generic_category

logger = logging.getLogger(__name__)

# Import conditionnel pour éviter les erreurs si harmonize.py n'existe pas encore
try:
    from services.harmonize import harmonize_products
    HARMONIZE_AVAILABLE = True
except ImportError:
    HARMONIZE_AVAILABLE = False

# Catégories génériques à affiner pour VVQM
VVQM_GENERIC_CATEGORIES = {
    'POISSON',
    'COQUILLAGES',
    'CRUSTACES',
    'CRUSTACES BRETONS',
    'FILETS',
}


def parse_vvqm_product_name(produit: str) -> dict:
    """
    Décompose un nom de produit VVQM en attributs structurés.

    Args:
        produit: Nom brut du produit (ex: "BAR DE LIGNE IKEJIME", "ST PIERRE PB Vidé")

    Returns:
        dict avec: Espece, Methode_Peche, Etat, Decoupe, Origine
    """
    result = {
        "Espece": None,
        "Methode_Peche": None,
        "Etat": None,
        "Decoupe": None,
        "Origine": None,
    }

    if not produit:
        return result

    produit_upper = produit.upper().strip()
    parts = produit_upper.split()

    if not parts:
        return result

    # 1. Extraire la découpe (en début de nom)
    if parts[0] in ['DOS', 'FILET', 'JOUE', 'LONGE']:
        result["Decoupe"] = parts[0]
        parts = parts[1:]

    if not parts:
        result["Espece"] = produit_upper
        return result

    # 2. Extraire méthode de pêche
    methode_parts = []
    if 'PB' in parts:
        methode_parts.append('PB')
        parts.remove('PB')
    if 'LIGNE' in parts:
        methode_parts.append('LIGNE')
        parts.remove('LIGNE')
    if 'DE' in parts and 'LIGNE' in methode_parts:
        parts.remove('DE')
    if 'IKEJIME' in parts:
        methode_parts.append('IKEJIME')
        parts.remove('IKEJIME')
    if 'IKE' in parts:
        methode_parts.append('IKEJIME')
        parts.remove('IKE')
    result["Methode_Peche"] = ' '.join(methode_parts) if methode_parts else None

    # 3. Extraire état/préparation
    etats_to_check = ['VIDÉ', 'VIDE', 'VIDÉE', 'CORAIL', 'BLANCHE', 'VIVANT', 'DÉC', 'ENTIERE', 'ENTIÈRE']
    for etat in etats_to_check:
        if etat in parts:
            result["Etat"] = etat.replace('VIDE', 'VIDÉ').replace('VIDÉE', 'VIDÉ').replace('ENTIERE', 'ENTIÈRE')
            parts.remove(etat)
            break

    # 4. Extraire origine
    if 'VAT' in parts:
        result["Origine"] = 'ATLANTIQUE'
        parts.remove('VAT')
    elif 'VDK' in parts:
        result["Origine"] = 'DANEMARK'
        parts.remove('VDK')

    # 5. Ce qui reste = espèce
    result["Espece"] = ' '.join(parts) if parts else produit_upper

    return result


def get_vvqm_category(espece: str) -> str:
    """
    Détermine la catégorie automatiquement basée sur l'espèce.

    Args:
        espece: Nom de l'espèce (ex: "BAR", "TURBOT")

    Returns:
        Catégorie (ex: "BAR", "TURBOT", "POISSON")
    """
    if not espece:
        return "POISSON"

    espece_upper = espece.upper()

    # Mappings PRIORITAIRES (patterns plus specifiques à vérifier en premier)
    priority_mappings = [
        ("ROUGET BARBET", "ROUGET BARBET"),
        ("ROUGET", "ROUGET BARBET"),
        ("BARBUE", "BARBUE"),  # Avant BAR car contient "BAR"
        ("LIEU JAUNE", "LIEU JAUNE"),
        ("LIEU NOIR", "LIEU NOIR"),
        ("ST PIERRE", "SAINT PIERRE"),
        ("SAINT PIERRE", "SAINT PIERRE"),
        ("COQUILLE ST JACQUES", "COQUILLE ST JACQUES"),
        ("NOIX ST JACQUES", "NOIX ST JACQUES"),
        ("NOIX SAINT JACQUES", "NOIX ST JACQUES"),
    ]

    for pattern, category in priority_mappings:
        if pattern in espece_upper:
            return category

    # Mapping standard espèce → catégorie
    mappings = {
        "BAR": "BAR",
        "TURBOT": "TURBOT",
        "MERLU": "MERLU",
        "MERLAN": "MERLAN",
        "CABILLAUD": "CABILLAUD",
        "SOLE": "SOLE",
        "DORADE": "DORADE",
        "LOTTE": "LOTTE",
        "BARBUE": "BARBUE",
        "CARRELET": "CARRELET",
        "MAIGRE": "MAIGRE",
        "GRONDIN": "GRONDIN",
        "RAIE": "RAIE",
        "LIMANDE": "LIMANDE",
        "ENCORNET": "ENCORNET",
        "POULPE": "POULPE",
        "SEICHE": "SEICHE",
        "CONGRE": "CONGRE",
        "PAGEOT": "PAGEOT",
        "PAGRE": "PAGRE",
        "JULIENNE": "JULIENNE",
        "SARDINE": "SARDINE",
        "MULET": "MULET",
        "VIVE": "VIVE",
        "SEBASTE": "SEBASTE",
        "BICHE": "BICHE",
        "EMISSOLE": "EMISSOLE",
        "ROUSSETTE": "ROUSSETTE",
        "MAQUEREAU": "MAQUEREAU",
        "THON": "THON",
        "ESPADON": "ESPADON",
        "ELINGUE": "ELINGUE",
        "BROSME": "BROSME",
        "MOSTELLE": "MOSTELLE",
        "GRENADIER": "GRENADIER",
        "SABRE": "SABRE",
        "ANON": "ANON",
        # Coquillages
        "COQUILLE": "COQUILLE ST JACQUES",
        "NOIX": "NOIX ST JACQUES",
        "COQUES": "COQUES",
        "PALOURDE": "PALOURDE",
        # Crustacés
        "ARAIGNEE": "ARAIGNEE",
        "TOURTEAU": "TOURTEAU",
        "HOMARD": "HOMARD",
        "LANGOUSTE": "LANGOUSTE",
        "CREVETTE": "CREVETTE",
        "BOUQUET": "BOUQUET",
    }

    for pattern, category in mappings.items():
        if pattern in espece_upper:
            return category

    return "POISSON"


def extract_data_from_pdf(file_bytes: bytes) -> pd.DataFrame:
    """
    Parse un PDF VVQM et extrait les données produits enrichies.

    Retourne un DataFrame avec colonnes:
    - keyDate, Vendor, ProductName, Code_Provider, Date, Prix, Categorie
    - Espece, Methode_Peche, Etat, Decoupe, Origine, Section, Calibre (enrichis)
    """
    doc = fitz.open(stream=file_bytes, filetype="pdf")

    def is_calibre_token(token):
        token = token.strip()
        return "/" in token or token == "0" or re.fullmatch(r"-?\d+", token)

    def is_valid_price_token(token):
        return bool(re.fullmatch(r"-?\d+(\.\d+)?", token.strip()))

    def clean_token(text):
        return text.replace("\xa0", " ").strip()

    def cluster_by_y(tokens, tolerance=1.5):
        clusters, current_cluster, current_y = [], [], None
        for y, x, token in sorted(tokens):
            if current_y is None or abs(y - current_y) <= tolerance:
                current_cluster.append((y, x, token))
                current_y = y if current_y is None else (current_y + y) / 2
            else:
                clusters.append(current_cluster)
                current_cluster = [(y, x, token)]
                current_y = y
        if current_cluster:
            clusters.append(current_cluster)
        return clusters

    tokens = []
    bold_prices = []
    date_pdf = None

    # Détection des sections (titres en gras, non-prix)
    section_titles = ['COQUILLAGES', 'CRUSTACES BRETONS', 'FILETS']
    sections = []  # [(y, section_name), ...]

    for page in doc:
        for block in page.get_text("dict")["blocks"]:
            if "lines" not in block:
                continue
            for line in block["lines"]:
                for span in line["spans"]:
                    x, y = span["bbox"][0], span["bbox"][1]
                    token = clean_token(span["text"])
                    font = span["font"]
                    if token:
                        tokens.append((y, x, token, font))
                        if is_valid_price_token(token) and "Bold" in font:
                            bold_prices.append((y, x, token))
                        # Détection des sections (gras, non-prix, y > 80)
                        elif "Bold" in font and y > 80:
                            token_upper = token.upper()
                            for section_title in section_titles:
                                if section_title in token_upper:
                                    sections.append((y, section_title))
                                    break
                        # Détection de la date
                        if not date_pdf:
                            m = re.match(r"\b(\d{2})\.(\d{2})\.(\d{4})\b", token)
                            if m:
                                jour, mois, annee = m.groups()
                                date_pdf = f"{annee}-{mois}-{jour}"

    # Trier les sections par Y pour pouvoir assigner la section à chaque produit
    sections = sorted(sections, key=lambda s: s[0])

    # Seuil X pour la colonne 4 (COQUILLAGES, CRUSTACES, FILETS)
    COLONNE_4_X_MIN = 500

    def get_section_for_y(y_pos, x_price):
        """
        Retourne la section pour une position Y donnée.
        Les sections s'appliquent UNIQUEMENT aux produits de la colonne 4 (x > 500).
        """
        if x_price < COLONNE_4_X_MIN:
            return None

        current_section = None
        for section_y, section_name in sections:
            if y_pos >= section_y:
                current_section = section_name
            else:
                break
        return current_section

    clean_tokens = [(y, x, token) for (y, x, token, _) in tokens]
    clusters = cluster_by_y(clean_tokens)

    used_prices = set()
    rows = []

    DIST_MAX_CALIBRE = 60  # px : seuil pour éviter de récupérer un calibre trop loin

    for cluster in clusters:
        tokens_sorted = sorted(cluster, key=lambda t: t[1])  # gauche → droite
        y_line = tokens_sorted[0][0]

        line_prices = [p for p in bold_prices if abs(p[0] - y_line) <= 1.5 and p not in used_prices]

        for y_price, x_price, val_price in sorted(line_prices, key=lambda p: p[1]):
            left_tokens = [t for t in tokens_sorted if t[1] < x_price]
            if not left_tokens:
                continue

            last = left_tokens[-1]
            last_token = last[2]
            dist_last = x_price - last[1]

            second_last = left_tokens[-2] if len(left_tokens) >= 2 else None
            dist_second_last = x_price - second_last[1] if second_last else None

            # Cas 1 : calibre immédiatement avant le prix
            if is_calibre_token(last_token) and dist_last < DIST_MAX_CALIBRE:
                calibre = last_token
                produit = left_tokens[-2][2] if len(left_tokens) >= 2 else ""

            # Cas 2 : calibre 2 tokens avant et proche
            elif second_last and is_calibre_token(second_last[2]) and dist_second_last < DIST_MAX_CALIBRE:
                calibre = second_last[2]
                produit = last_token

            # Cas 3 : Produit + Prix (colonne 4 sans calibre)
            else:
                calibre = ""
                produit = last_token

            # Déterminer la section pour ce produit (seulement colonne 4)
            section = get_section_for_y(y_line, x_price)

            rows.append({
                "Produit": produit.strip(),
                "Calibre": calibre.strip(),
                "Prix": val_price.strip(),
                "y_line": y_line,
                "Section": section
            })
            used_prices.add((y_price, x_price, val_price))

    df_final = pd.DataFrame(rows).drop_duplicates(subset=["Produit", "Calibre", "Prix"])
    df_final["Date"] = date_pdf
    df_final["Prix"] = df_final["Prix"].apply(lambda x: None if x == "" else x)
    df_final["Vendor"] = "VVQM"
    df_final["Code_Provider"] = (
        df_final["Vendor"] + "__" + df_final["Produit"] + "__" + df_final["Calibre"]
    ).str.replace(" ", "_")
    df_final["Code_Provider"] = df_final["Code_Provider"].str.replace("__", "_")

    df_final["ProductName"] = df_final.apply(
        lambda r: r["Produit"] if r["Calibre"] == "" else f"{r['Produit']} - {r['Calibre']}", axis=1
    )
    df_final["keyDate"] = df_final["Code_Provider"] + "_" + df_final["Date"]

    # Enrichissement avec parse_vvqm_product_name()
    def enrich_product(row):
        parsed = parse_vvqm_product_name(row["Produit"])
        return pd.Series(parsed)

    enriched = df_final.apply(enrich_product, axis=1)
    df_final = pd.concat([df_final, enriched], axis=1)

    # Catégorisation automatique
    def compute_category(row):
        # Priorité: section PDF > mapping espèce > défaut
        if row["Section"]:
            return row["Section"]
        return get_vvqm_category(row["Espece"])

    df_final["Categorie"] = df_final.apply(compute_category, axis=1)

    # Colonnes de sortie enrichies
    output_cols = [
        "keyDate", "Vendor", "ProductName", "Code_Provider", "Date", "Prix", "Categorie",
        "Espece", "Methode_Peche", "Etat", "Decoupe", "Origine", "Section", "Calibre"
    ]

    return df_final[output_cols]


def parse(file_bytes: bytes, harmonize: bool = False, **kwargs) -> list[dict]:
    """
    Point d'entrée principal du parser VVQM.

    Extrait les produits d'un PDF VVQM et optionnellement
    applique les règles d'harmonisation pour normaliser les attributs.

    Args:
        file_bytes: Contenu binaire du fichier PDF
        harmonize: Si True, applique l'harmonisation des attributs
                   (normalise les valeurs selon docs/harmonisation_attributs.md)
        **kwargs: Arguments supplémentaires (réservés pour extensions futures)

    Returns:
        Liste de dictionnaires produits avec les champs:
        - Date, Vendor, keyDate, Code_Provider, Prix, ProductName
        - Categorie, Methode_Peche, Decoupe, Etat, Calibre
        - Espece (espèce extraite du nom)

        Si harmonize=True, les clés sont en snake_case et les valeurs normalisées:
        - categorie (via mapping espèce)
        - methode_peche (LIGNE extrait de "LIGNE IKEJIME")
        - etat (VIDÉ → VIDE, CORAIL → CORAILLE)
        - technique_abattage (IKEJIME extrait de "LIGNE IKEJIME")
        - calibre (virgules → points)

    Example:
        >>> with open("cours_vvqm.pdf", "rb") as f:
        ...     products = parse(f.read(), harmonize=True)
        >>> products[0]["technique_abattage"]
        'IKEJIME'  # Extrait de 'LIGNE IKEJIME'
    """
    # Extraction des données brutes
    df = extract_data_from_pdf(file_bytes)
    products = sanitize_for_json(df)

    # Affinage des catégories génériques vers espèces spécifiques
    for product in products:
        product["Categorie"] = refine_generic_category(
            product.get("Categorie"),
            product.get("ProductName"),
            VVQM_GENERIC_CATEGORIES
        )

    # Application optionnelle de l'harmonisation
    if harmonize:
        if not HARMONIZE_AVAILABLE:
            raise ImportError(
                "Le module services.harmonize n'est pas disponible. "
                "Vérifiez que services/harmonize.py existe."
            )
        products = harmonize_products(products, vendor="VVQM")

    return products


# Alias pour compatibilité
parse_vvqm = parse
