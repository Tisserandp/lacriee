"""
Parser pour les PDF Laurent Daniel (LD).

Structure du document:
- 3 colonnes de produits par page
- Catégories en MAJUSCULES décalées à droite
- Produits avec nom, qualité et prix sur la même ligne
- Date au format "DD mois YYYY" en haut

Attributs extraits:
- Categorie: Déduite du préfixe du nom produit ou des sections
- Methode_Peche: LIGNE, PB, DK, CHALUT, PLONGEE
- Qualite: EXTRA, SUP, XX, SF (Sans Flanc), PREMIUM
- Decoupe: FILET, QUEUE, DOS, DARNE, PAVE, AILE, CHAIR, BLANC
- Etat: PELEE, GLACE, VIVANT, ROUGE, BLANCHE, NOIRE, CUIT, VIDEE
- Origine: ROSCOFF, BRETON, ECOSSE, GLENAN, FRANCE, IRLANDE, NORVEGE
- Calibre: Plages numériques (1/2, 500/600), format plus (500+)

Harmonisation:
- Ce parseur supporte l'harmonisation via services/harmonize.py
- Utiliser parse(file_bytes, harmonize=True) pour obtenir des attributs normalisés
- Voir docs/harmonisation_attributs.md pour les règles de normalisation

Spécificités:
- Les couleurs (ROUGE, BLANCHE, NOIRE) sont extraites vers le champ 'couleur'
  lors de l'harmonisation (spécifique langoustines/crevettes)
"""
from io import BytesIO
import fitz
import pandas as pd
import numpy as np
import re
from datetime import date
from parsers.utils import sanitize_for_json, refine_generic_category
import logging

logger = logging.getLogger(__name__)

# Catégories génériques à affiner pour Laurent Daniel
LAURENT_DANIEL_GENERIC_CATEGORIES = {'COQUILLAGES', 'DIVERS', 'FILET'}

# Import conditionnel pour éviter les erreurs si harmonize.py n'existe pas encore
try:
    from services.harmonize import harmonize_products
    HARMONIZE_AVAILABLE = True
except ImportError:
    HARMONIZE_AVAILABLE = False


def parse_laurent_daniel_attributes(product_name: str, categorie: str = None) -> dict:
    """
    Extrait les attributs structurés depuis ProductName pour Laurent Daniel.

    Args:
        product_name: Nom complet du produit (ex: "Bar 3/4 LIGNE")
        categorie: Catégorie du produit (optionnel)

    Returns:
        dict avec: Methode_Peche, Qualite, Decoupe, Etat, Origine, Calibre, Infos_Brutes
    """
    result = {
        "Methode_Peche": None,
        "Qualite": None,
        "Decoupe": None,
        "Etat": None,
        "Origine": None,
        "Calibre": None,
        "Infos_Brutes": None,
    }

    if not product_name:
        return result

    text_upper = product_name.upper()
    infos_trouvees = []

    # --- Méthode de pêche ---
    methode_patterns = [
        (r'\bLIGNE\b', 'LIGNE'),
        (r'\bPB\b', 'PB'),  # Petit Bateau
        (r'\bDK\b', 'DK'),
        (r'\bCHALUT\b', 'CHALUT'),
        (r'\bPLONGEE\b', 'PLONGEE'),
    ]
    for pattern, method in methode_patterns:
        if re.search(pattern, text_upper):
            result["Methode_Peche"] = method
            infos_trouvees.append(f"Méthode:{method}")
            break

    # --- Qualité ---
    qualite_patterns = [
        (r'\bEXTRA\b', 'EXTRA'),
        (r'\bSUP\b', 'SUP'),
        (r'\bXX\b', 'XX'),
        (r'\bSF\b', 'SF'),  # Sans Flanc
        (r'\bPREMIUM\b', 'PREMIUM'),
    ]
    for pattern, qualite in qualite_patterns:
        if re.search(pattern, text_upper):
            result["Qualite"] = qualite
            infos_trouvees.append(f"Qualité:{qualite}")
            break

    # --- Découpe ---
    decoupe_patterns = [
        (r'\bFILET\b', 'FILET'),
        (r'\bQUEUE\b', 'QUEUE'),
        (r'\bDOS\b', 'DOS'),
        (r'\bDARNE\b', 'DARNE'),
        (r'\bPAVE\b', 'PAVE'),
        (r'\bAILE\b', 'AILE'),
        (r'\bCHAIR\b', 'CHAIR'),
        (r'\bBLANC\b', 'BLANC'),  # Blanc de seiche
    ]
    for pattern, decoupe in decoupe_patterns:
        if re.search(pattern, text_upper):
            result["Decoupe"] = decoupe
            infos_trouvees.append(f"Découpe:{decoupe}")
            break

    # --- État/Conservation ---
    etat_patterns = [
        (r'\bPELEE?\b', 'PELEE'),
        (r'\bGLACE\b', 'GLACE'),
        (r'\bVIVANT[ES]?\b', 'VIVANT'),
        (r'\bROUGE\b', 'ROUGE'),
        (r'\bBLANCHE\b', 'BLANCHE'),
        (r'\bNOIRE?\b', 'NOIRE'),
        (r'\bCUIT[ES]?\b', 'CUIT'),
        (r'\bVIDEE?\b', 'VIDEE'),
    ]
    for pattern, etat in etat_patterns:
        if re.search(pattern, text_upper):
            result["Etat"] = etat
            infos_trouvees.append(f"État:{etat}")
            break

    # --- Origine ---
    origine_patterns = [
        (r'\bROSCOFF\b', 'ROSCOFF'),
        (r'\bBRETON\b', 'BRETON'),
        (r'\bECOSSE\b', 'ECOSSE'),
        (r'\bGLENAN\b', 'GLENAN'),
        (r'\bFRANCE\b', 'FRANCE'),
        (r'\bIRLANDE\b', 'IRLANDE'),
        (r'\bNORVEGE\b', 'NORVEGE'),
    ]
    origines_trouvees = []
    for pattern, origine in origine_patterns:
        if re.search(pattern, text_upper) and origine not in origines_trouvees:
            origines_trouvees.append(origine)
            infos_trouvees.append(f"Origine:{origine}")
    if origines_trouvees:
        result["Origine"] = ", ".join(origines_trouvees)

    # --- Calibre ---
    calibre_trouve = None

    # Pattern 1: Plages numériques (1/2, 4/600, 1.5/2, 800/+, 500+)
    match_plage = re.search(r'\b(\d+(?:[,.]?\d*)?)\s*/\s*(\d+(?:[,.]?\d*)?|\+)', text_upper)
    if match_plage:
        calibre_trouve = f"{match_plage.group(1)}/{match_plage.group(2)}"

    # Pattern 2: Format simple avec + (500+, 800+)
    if not calibre_trouve:
        match_plus = re.search(r'\b(\d+)\+\b', text_upper)
        if match_plus:
            calibre_trouve = f"{match_plus.group(1)}+"

    # Pattern 3: Poids simple (500gr, 2kg)
    if not calibre_trouve:
        match_poids = re.search(r'\b(\d+)\s*(GR|KG)\b', text_upper)
        if match_poids:
            calibre_trouve = f"{match_poids.group(1)}{match_poids.group(2).lower()}"

    if calibre_trouve:
        result["Calibre"] = calibre_trouve
        infos_trouvees.append(f"Calibre:{calibre_trouve}")

    # --- Construction Infos_Brutes ---
    if infos_trouvees:
        result["Infos_Brutes"] = " | ".join(infos_trouvees)

    return result


def extract_data_from_pdf(file_bytes: bytes) -> list[dict]:
    """
    Extrait les données produits et la date d'un PDF LD, renvoie une liste JSON-ready.
    Prend en entrée les bytes du fichier PDF.
    Version avec coordonnées relatives (ratios) - indépendante de la résolution.
    """
    # Open PDF from bytes
    doc = fitz.open(stream=BytesIO(file_bytes), filetype="pdf")

    # Récupération des dimensions de page pour calculs relatifs
    page_width = doc[0].rect.width if len(doc) > 0 else 595.32
    page_height = doc[0].rect.height if len(doc) > 0 else 841.92

    # --- DÉTECTION DU FORMAT PDF (A4 ou A3) ---
    # A4: ~595 points de large, A3: ~841 points de large
    is_a3_format = page_width >= 750

    # --- CONSTANTES DE RATIOS (avec support A4 et A3) ---
    if is_a3_format:
        # Ratios calibrés sur CC.pdf (A3: 841.32 x 1190.32)
        COL1_X_MIN_RATIO = 0.000000
        COL1_X_MAX_RATIO = 0.305000
        COL2_X_MIN_RATIO = 0.306000
        COL2_X_MAX_RATIO = 0.555000
        COL3_X_MIN_RATIO = 0.556000
        COL3_X_MAX_RATIO = 1.000000

        PRIX_COL0_MIN_RATIO = 0.190177
        PRIX_COL0_MAX_RATIO = 0.231779
        PRIX_COL1_MIN_RATIO = 0.445728
        PRIX_COL1_MAX_RATIO = 0.487329
        PRIX_COL2_MIN_RATIO = 0.707222
        PRIX_COL2_MAX_RATIO = 0.742880

        QUALITE_COL0_MIN_RATIO = 0.249608
        QUALITE_COL0_MAX_RATIO = 0.291209
        QUALITE_COL1_MIN_RATIO = 0.499216
        QUALITE_COL1_MAX_RATIO = 0.540817
        QUALITE_COL2_MIN_RATIO = 0.754766

        CATEGORIE_COL0_MIN_RATIO = 0.061808
        CATEGORIE_COL2_MIN_RATIO = 0.564589

        # Y_MIN ajusté pour capturer les catégories (12.94% pour CC.pdf)
        Y_MIN_RATIO = 0.100000
    else:
        # Ratios calibrés sur CC2.pdf (A4: 595.32 x 841.92)
        COL1_X_MIN_RATIO = 0.000000
        COL1_X_MAX_RATIO = 0.319156
        COL2_X_MIN_RATIO = 0.320836
        COL2_X_MAX_RATIO = 0.571121
        COL3_X_MIN_RATIO = 0.572801
        COL3_X_MAX_RATIO = 1.000000

        PRIX_COL0_MIN_RATIO = 0.223409
        PRIX_COL0_MAX_RATIO = 0.251965
        PRIX_COL1_MIN_RATIO = 0.472015
        PRIX_COL1_MAX_RATIO = 0.502251
        PRIX_COL2_MIN_RATIO = 0.673587
        PRIX_COL2_MAX_RATIO = 0.772694

        QUALITE_COL0_MIN_RATIO = 0.253645
        QUALITE_COL0_MAX_RATIO = 0.319156
        QUALITE_COL1_MIN_RATIO = 0.503931
        QUALITE_COL1_MAX_RATIO = 0.571121
        QUALITE_COL2_MIN_RATIO = 0.774373

        CATEGORIE_COL0_MIN_RATIO = 0.092387
        CATEGORIE_COL2_MIN_RATIO = 0.604717

        # Y_MIN ajusté pour capturer les catégories (14.93% pour CC2.pdf)
        Y_MIN_RATIO = 0.140000

    # --------------------- Extraction des lignes et de la date ---------------------
    raw = []
    for page in doc:
        raw += [line.strip() for line in page.get_text().splitlines() if line.strip()]
    date_str = None
    date_pattern = re.compile(r"(\d{1,2})\s+([a-zéû]+)\s+(\d{4})", re.IGNORECASE)
    mois_fr = {
        "janvier": 1, "février": 2, "mars": 3, "avril": 4,
        "mai": 5, "juin": 6, "juillet": 7, "août": 8,
        "septembre": 9, "octobre": 10, "novembre": 11, "décembre": 12
    }
    for i, line in enumerate(raw):
        match = date_pattern.search(line)
        if match:
            jour, mois_str, annee = match.groups()
            mois = mois_fr.get(mois_str.lower(), None)
            if mois:
                date_obj = date(int(annee), mois, int(jour))
                date_str = date_obj.isoformat()

    # ---------------------- Extraction des mots et positions -----------------------
    # Étape 1 : Mots avec coordonnées précises
    raw_words = []
    for page_num, page in enumerate(doc):
        for w in page.get_text("words"):
            x0, y0, x1, y1, word, block_no, line_no, word_no = w
            raw_words.append({
                'page': page_num,
                'x0': x0, 'y0': y0, 'x1': x1, 'y1': y1,
                'word': word.strip(),
                'center_x': (x0 + x1) / 2,
                'center_y': (y0 + y1) / 2,
                'is_bold': False  # Default
            })

    # Étape 2 : Zones en gras
    bold_zones = []
    for page_num, page in enumerate(doc):
        blocks = page.get_text("dict")["blocks"]
        for b in blocks:
            if "lines" not in b:
                continue
            for l in b["lines"]:
                for s in l["spans"]:
                    flags = s["flags"]
                    font_name = s["font"]
                    is_bold = bool(flags & 16) or "Bold" in font_name or "Black" in font_name
                    if is_bold:
                        bbox = s["bbox"]
                        bold_zones.append({
                            'page': page_num,
                            'x0': bbox[0], 'y0': bbox[1], 'x1': bbox[2], 'y1': bbox[3]
                        })

    # Étape 3 : Croisement
    # Un mot est gras si son centre est dans une zone grasse de la même page
    for w in raw_words:
        w_page = w['page']
        cx, cy = w['center_x'], w['center_y']
        for bz in bold_zones:
            if bz['page'] == w_page:
                # Tolérance légère sur les bords
                if (bz['x0'] <= cx <= bz['x1']) and (bz['y0'] - 2 <= cy <= bz['y1'] + 2):
                    w['is_bold'] = True
                    break

    coords_df = pd.DataFrame(raw_words)
    # Filtrage Y_MIN et EURO/KG
    coords_df = coords_df[
        (coords_df['y0'] >= page_height * Y_MIN_RATIO) &
        (coords_df['word'].str.upper() != 'EURO/KG')
    ].reset_index(drop=True)

    # -------------- Définition des colonnes et fonctions de matching ---------------
    COLS = [
        {'name': 'col1', 'x_min': page_width * COL1_X_MIN_RATIO, 'x_max': page_width * COL1_X_MAX_RATIO},
        {'name': 'col2', 'x_min': page_width * COL2_X_MIN_RATIO, 'x_max': page_width * COL2_X_MAX_RATIO},
        {'name': 'col3', 'x_min': page_width * COL3_X_MIN_RATIO, 'x_max': page_width * COL3_X_MAX_RATIO},
    ]

    def is_prix(x0, col_idx):
        if col_idx == 0:
            return (page_width * PRIX_COL0_MIN_RATIO) <= x0 <= (page_width * PRIX_COL0_MAX_RATIO)
        if col_idx == 1:
            return (page_width * PRIX_COL1_MIN_RATIO) <= x0 <= (page_width * PRIX_COL1_MAX_RATIO)
        if col_idx == 2:
            return (page_width * PRIX_COL2_MIN_RATIO) <= x0 <= (page_width * PRIX_COL2_MAX_RATIO)
        return False

    def is_qualite(x0, col_idx):
        if col_idx == 0:
            return (page_width * QUALITE_COL0_MIN_RATIO) <= x0 <= (page_width * QUALITE_COL0_MAX_RATIO)
        if col_idx == 1:
            return (page_width * QUALITE_COL1_MIN_RATIO) <= x0 <= (page_width * QUALITE_COL1_MAX_RATIO)
        if col_idx == 2:
            return x0 > (page_width * QUALITE_COL2_MIN_RATIO)
        return False

    def is_categorie(words, is_bold_list, col_idx):
        # Règle stricte : Categorie = GRAS et MAJUSCULES (et pas trop long)
        if not any(is_bold_list):
            return False

        # On garde la validation MAJUSCULES pour écarter d'éventuels parasites gras
        excluded = ['PB', 'LIGNE', 'DK', 'CHALUT', 'ROUGE', 'BLANCHE', 'GLACE', 'EXTRA', 'XX', 'SF', 'SV', 'AV']
        cat_candidates = [w for w in words if w.isupper() and w not in excluded and w != "-"]

        # Si on a des mots en majuscules ET du gras
        if len(cat_candidates) >= 1 and any(is_bold_list):
            return True

        return False

    # ------------------------- Extraction des produits -----------------------------
    results = []
    for col_idx, col in enumerate(COLS):
        col_df = coords_df[(coords_df['x0'] >= col['x_min']) & (coords_df['x0'] <= col['x_max'])].copy()
        col_df = col_df.sort_values(['y0', 'x0']).reset_index(drop=True)
        cat = None
        for y0, group in col_df.groupby('y0'):
            words = list(group['word'])
            x0s = list(group['x0'])
            bolds = list(group['is_bold'])

            if is_categorie(words, bolds, col_idx):
                cat_words = [w for w in words if w != "-"]
                if cat_words:
                    cat = " ".join(cat_words).strip("- ").strip()
                continue

            produit_mots = []
            prix = ""
            qualite = ""
            for w, x0 in zip(words, x0s):
                if is_prix(x0, col_idx):
                    prix = w
                elif is_qualite(x0, col_idx):
                    qualite = w
                else:
                    produit_mots.append(w)
            produit = " ".join(produit_mots)
            if produit:
                results.append({
                    'colonne': col['name'],
                    'categorie': cat,
                    'produit': produit,
                    'prix': prix,
                    'qualite': qualite
                })
    df_final = pd.DataFrame(results)
    if df_final.empty:
        raise ValueError("Aucun produit détecté dans le PDF.")

    # --------------------- Nettoyage et enrichissement final ----------------------
    df_final['Prix'] = (
        df_final['prix']
        .replace("-", "")
        .replace("", np.nan)
        .str.replace(",", ".", regex=False)
        .astype(float)
    )
    df_final = df_final.fillna("")
    df_final['produit_lower'] = df_final['produit'].str.lower()
    df_final['categorie'] = df_final['categorie'].str.lower()
    rules = [
        ('lieu jaune', 'lieu'),
        ('cabillaud', 'cabillaud'),
        ('anon', 'anon'),
        ('carrelet', 'carrelet'),
        ('sardine', 'sardine'),
        ('maquereaux', 'maquereaux'),
        ('merou', 'merou'),
        ('merlan', 'merlan'),
        ('maigre', 'maigre'),
        ('saumon', 'saumon'),
        ('st pierre', 'SAINT PIERRE')
    ]
    for prefix, cat in rules:
        mask = df_final['produit_lower'].str.startswith(prefix)
        df_final.loc[mask, 'categorie'] = cat
    df_final['Categorie'] = df_final['categorie'].str.upper()

    # Affinage des catégories génériques vers espèces spécifiques
    df_final['Categorie'] = df_final.apply(
        lambda row: refine_generic_category(
            row['Categorie'],
            row['produit'],
            LAURENT_DANIEL_GENERIC_CATEGORIES
        ),
        axis=1
    )

    df_final = df_final.drop(columns=['produit_lower'])
    df_final['Code_Provider'] = 'LD_' + df_final['produit'].str.replace(" ", "") + "_" + df_final["qualite"]
    df_final['Date'] = date_str
    df_final['Vendor'] = "Laurent Daniel"
    df_final["keyDate"] = df_final["Code_Provider"] + "_" + str(date_str)
    df_final["ProductName"] = df_final["produit"] + " " + df_final["qualite"]

    # ---------- Enrichissement des attributs depuis ProductName ----------------------
    enriched_attributes = []
    for _, row in df_final.iterrows():
        attrs = parse_laurent_daniel_attributes(row["ProductName"], row["Categorie"])
        enriched_attributes.append(attrs)

    attrs_df = pd.DataFrame(enriched_attributes)
    for col in attrs_df.columns:
        df_final[col] = attrs_df[col]

    df_final2 = df_final[['Date', 'Vendor', "keyDate", 'Code_Provider', 'Prix', 'ProductName', "Categorie",
                          'Methode_Peche', 'Qualite', 'Calibre', 'Decoupe', 'Etat', 'Origine', 'Infos_Brutes']]

    # ---------- Appel de la fonction de sanitization/JSON + gestion d'erreur -------
    try:
        json_ready = sanitize_for_json(df_final2)
        logger.info("Conversion JSON OK")
        return json_ready
    except Exception:
        logger.exception("Erreur lors du `sanitize_for_json`")
        raise


def parse(file_bytes: bytes, harmonize: bool = False, **kwargs) -> list[dict]:
    """
    Point d'entrée principal du parser Laurent Daniel.

    Extrait les produits d'un PDF Laurent Daniel et optionnellement
    applique les règles d'harmonisation pour normaliser les attributs.

    Args:
        file_bytes: Contenu binaire du fichier PDF
        harmonize: Si True, applique l'harmonisation des attributs
                   (normalise les valeurs selon docs/harmonisation_attributs.md)
        **kwargs: Arguments supplémentaires (réservés pour extensions futures)

    Returns:
        Liste de dictionnaires produits avec les champs:
        - Date, Vendor, keyDate, Code_Provider, Prix, ProductName
        - Categorie, Methode_Peche, Qualite, Decoupe, Etat, Origine, Calibre
        - Infos_Brutes (concaténation des attributs extraits)

        Si harmonize=True, les clés sont en snake_case et les valeurs normalisées:
        - categorie, methode_peche, qualite, decoupe, etat, origine, calibre
        - couleur (extrait de etat pour ROUGE, BLANCHE, NOIRE)

    Example:
        >>> with open("cours_ld.pdf", "rb") as f:
        ...     products = parse(f.read(), harmonize=True)
        >>> products[0]["origine"]
        'BRETAGNE'  # Normalisé depuis 'BRETON'
    """
    # Extraction des données brutes
    products = extract_data_from_pdf(file_bytes)

    # Application optionnelle de l'harmonisation
    if harmonize:
        if not HARMONIZE_AVAILABLE:
            raise ImportError(
                "Le module services.harmonize n'est pas disponible. "
                "Vérifiez que services/harmonize.py existe."
            )
        products = harmonize_products(products, vendor="Laurent Daniel")

    return products


# Alias pour compatibilité
parse_laurent_daniel = parse
