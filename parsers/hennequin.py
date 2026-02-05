"""
Parser pour les PDF Hennequin.

Structure du document:
- 2 colonnes de produits par page
- Catégories identifiées par position X spécifique
- Produits avec qualité et prix sur colonnes séparées
- Enrichissement via table de référence BigQuery

Attributs extraits:
- Categorie: Via positions X dans le PDF
- Methode_Peche: PT BATEAU, LIGNE, SENNEUR, SAUVAGE, PECHE LOCALE, CASIER, CHALUT, PALANGRE, FILEYEUR
- Qualite: EXTRA PINS, QUALITE PREMIUM, EXTRA, SUP
- Decoupe: FILET, QUEUE, AILE, LONGE, PINCE, CUISSES, FT, DOS
- Etat: VIDEE, PELEE, CORAILLEES, DEGRESSEE, DESARETEE, VIVANT, CUIT, DECORTIQUEES
- Origine: FAO zones (FAO87, FAO27), pays (FRANCE, ECOSSE, NORVEGE...), régions (VENDEE, BRETAGNE)
- Calibre: Plages, N° huîtres, mots-clés (JUMBO, XXL, GEANT)
- Conservation: SURGELEE, CONGELEE, IQF, FRAIS (spécifique Hennequin)

Harmonisation:
- Ce parseur supporte l'harmonisation via services/harmonize.py
- Utiliser parse(file_bytes, harmonize=True) pour obtenir des attributs normalisés
- Voir docs/harmonisation_attributs.md pour les règles de normalisation

Spécificités:
- PT BATEAU est normalisé en PB
- SAUVAGE est extrait vers le champ 'type_production'
- Conservation est un attribut spécifique à Hennequin
"""
import re
import logging
from datetime import datetime
from collections import defaultdict

import fitz  # PyMuPDF
import numpy as np
import pandas as pd

from parsers.utils import refine_generic_category

logger = logging.getLogger(__name__)

# Catégories génériques à affiner pour Hennequin
HENNEQUIN_GENERIC_CATEGORIES = {
    'COUPE "FAIT MAISON"',
    'COUPE FAIT MAISON',
    'COUPE " FAIT MAISON "',  # Variante avec espaces
    'CUISSON',
    'VIVIERS',
    'COQUILLAGES',
}

# Import conditionnel pour éviter les erreurs si harmonize.py n'existe pas encore
try:
    from services.harmonize import harmonize_products
    HARMONIZE_AVAILABLE = True
except ImportError:
    HARMONIZE_AVAILABLE = False


def sanitize_for_json(df: pd.DataFrame) -> list[dict]:
    """
    Convertit un DataFrame en liste de dicts JSON-safe.
    Gère les types numpy et les valeurs NaN/None.
    """
    result = []
    for _, row in df.iterrows():
        record = {}
        for col in df.columns:
            val = row[col]
            if pd.isna(val):
                record[col] = None
            elif isinstance(val, (np.integer, np.int64)):
                record[col] = int(val)
            elif isinstance(val, (np.floating, np.float64)):
                if np.isinf(val):
                    record[col] = None
                else:
                    record[col] = float(val)
            elif isinstance(val, np.bool_):
                record[col] = bool(val)
            elif hasattr(val, 'isoformat'):
                record[col] = val.isoformat()
            else:
                record[col] = val
        result.append(record)
    return result


def parse_hennequin_attributes(product_name: str, categorie: str = None) -> dict:
    """
    Extrait les attributs structurés depuis ProductName et Categorie pour Hennequin.

    Args:
        product_name: Nom complet du produit (ex: "BAR PT BATEAU 500/1000 EXTRA SENNEUR")
        categorie: Catégorie du produit (ex: "BAR", "CABILLAUD")

    Returns:
        dict avec: Methode_Peche, Qualite, Decoupe, Etat, Conservation, Origine, Infos_Brutes
    """
    result = {
        "Methode_Peche": None,
        "Qualite": None,
        "Decoupe": None,
        "Etat": None,
        "Conservation": None,
        "Origine": None,
        "Calibre": None,
        "Infos_Brutes": None,
    }

    if not product_name:
        return result

    # Combiner ProductName et Categorie pour la recherche
    text_combined = product_name.upper()
    if categorie:
        text_combined = f"{categorie.upper()} {text_combined}"

    # Liste pour collecter tous les attributs trouvés
    infos_trouvees = []

    # --- Méthodes de pêche ---
    methode_patterns = [
        (r'\bPT\s+BATEAU\b', 'PT BATEAU'),
        (r'\bPETIT\s+BATEAU\b', 'PT BATEAU'),
        (r'\bDE\s+LIGNE\b', 'LIGNE'),
        (r'\bLIGNE\b', 'LIGNE'),
        (r'\bSENNEUR\b', 'SENNEUR'),
        (r'\bSAUVAGE\b', 'SAUVAGE'),
        (r'\bPECHE\s+LOCALE\b', 'PECHE LOCALE'),
        (r'\bCASIER\b', 'CASIER'),
        (r'\bCHALUT\b', 'CHALUT'),
        (r'\bPALANGRE\b', 'PALANGRE'),
        (r'\bFILEYEUR\b', 'FILEYEUR'),
    ]
    for pattern, method in methode_patterns:
        if re.search(pattern, text_combined):
            if result["Methode_Peche"] is None:
                result["Methode_Peche"] = method
            infos_trouvees.append(f"Méthode:{method}")
            break  # Prendre la première méthode trouvée

    # --- Qualité ---
    qualite_patterns = [
        (r'\bEXTRA\s+PINS?\b', 'EXTRA PINS'),
        (r'\bQUALITE\s+PREMIUM\b', 'QUALITE PREMIUM'),
        (r'\bEXTRA\b', 'EXTRA'),
        (r'\bSUP\b', 'SUP'),
    ]
    for pattern, qualite in qualite_patterns:
        if re.search(pattern, text_combined):
            if result["Qualite"] is None:
                result["Qualite"] = qualite
            infos_trouvees.append(f"Qualité:{qualite}")
            break

    # --- Découpe ---
    decoupe_patterns = [
        (r'\bFILET\b', 'FILET'),
        (r'\bQUEUE\b', 'QUEUE'),
        (r'\bAILE\b', 'AILE'),
        (r'\bLONGE\b', 'LONGE'),
        (r'\bPINCE\b', 'PINCE'),
        (r'\bCUISSES?\b', 'CUISSES'),
        (r'\bFT\b', 'FILET'),  # FT = Filet
        (r'\bDOS\b', 'DOS'),
    ]
    for pattern, decoupe in decoupe_patterns:
        if re.search(pattern, text_combined):
            if result["Decoupe"] is None:
                result["Decoupe"] = decoupe
            infos_trouvees.append(f"Découpe:{decoupe}")
            break

    # --- État/Préparation ---
    etat_patterns = [
        (r'\bVIDEE?\b', 'VIDEE'),
        (r'\bPELEE?\b', 'PELEE'),
        (r'\bCORAILLEE?S?\b', 'CORAILLEES'),
        (r'\bDEGRESSE?E?\b', 'DEGRESSEE'),
        (r'\bDESARETE?E?\b', 'DESARETEE'),
        (r'\bVIVANT\b', 'VIVANT'),
        (r'\bCUITE?S?\b', 'CUIT'),
        (r'\bDECORTIQUEE?S?\b', 'DECORTIQUEES'),
    ]
    for pattern, etat in etat_patterns:
        if re.search(pattern, text_combined):
            if result["Etat"] is None:
                result["Etat"] = etat
            infos_trouvees.append(f"État:{etat}")
            break

    # --- Conservation ---
    conservation_patterns = [
        (r'\bSURGELEE?S?\b', 'SURGELEE'),
        (r'\bCONGELEE?S?\b', 'CONGELEE'),
        (r'\bIQF\b', 'IQF'),
        (r'\bFRAIS\b', 'FRAIS'),
    ]
    for pattern, conservation in conservation_patterns:
        if re.search(pattern, text_combined):
            if result["Conservation"] is None:
                result["Conservation"] = conservation
            infos_trouvees.append(f"Conservation:{conservation}")
            break

    # --- Origine (pays, régions, zones FAO) ---
    origine_patterns = [
        # Zones FAO (spécifiques d'abord)
        (r'\bFAO\s*87\b', 'FAO87'),
        (r'\bFAO\s*27\b', 'FAO27'),
        # Pays/Régions
        (r'\bFRANCE\b', 'FRANCE'),
        (r'\bVENDEE\b', 'VENDEE'),
        (r'\bBRETAGNE\b', 'BRETAGNE'),
        (r'\bILES?\s+FEROE\b', 'ILES FEROE'),
        (r'\bECOSSE\b', 'ECOSSE'),
        (r'\bMADAGASCAR\b', 'MADAGASCAR'),
        (r'\bVIETNAM\b', 'VIETNAM'),
        (r'\bEQUATEUR\b', 'EQUATEUR'),
        (r'\bNORVEGE\b', 'NORVEGE'),
        (r'\bESPAGNE\b', 'ESPAGNE'),
        (r'\bPORTUGAL\b', 'PORTUGAL'),
        (r'\bIRLANDE\b', 'IRLANDE'),
        (r'\bVAT\b', 'ATLANTIQUE'),  # VAT = Atlantique
    ]
    origines_trouvees = []
    for pattern, origine in origine_patterns:
        match = re.search(pattern, text_combined)
        if match and origine not in origines_trouvees:
            origines_trouvees.append(origine)
            infos_trouvees.append(f"Origine:{origine}")

    if origines_trouvees:
        result["Origine"] = ", ".join(origines_trouvees)

    # --- Calibre (extraction depuis ProductName uniquement) ---
    # On cherche dans product_name original (pas text_combined) pour plus de précision
    product_upper = product_name.upper() if product_name else ""

    calibre_trouve = None

    # Pattern 1: Plages numériques (1/2, 500/1000, 800/1.2, 1.8/2.5)
    match_plage = re.search(r'\b(\d+(?:\.\d+)?)\s*/\s*(\d+(?:\.\d+)?)\b', product_upper)
    if match_plage:
        calibre_trouve = f"{match_plage.group(1)}/{match_plage.group(2)}"

    # Pattern 2: Calibres "Plus" (+1, +2, +1.5)
    if not calibre_trouve:
        match_plus = re.search(r'(\+\d+(?:\.\d+)?)\b', product_upper)
        if match_plus:
            calibre_trouve = match_plus.group(1)

    # Pattern 3: Calibres huîtres (N°1, N°2, N° 3)
    if not calibre_trouve:
        match_huitre = re.search(r'\b(N°\s?\d+)\b', product_upper)
        if match_huitre:
            calibre_trouve = match_huitre.group(1).replace(' ', '')

    # Pattern 4: Calibres textuels (mots-clés)
    if not calibre_trouve:
        calibre_keywords = [
            (r'\bJUMBO\b', 'JUMBO'),
            (r'\bXXL\b', 'XXL'),
            (r'\bXL\b', 'XL'),
            (r'\bGEANTS?\b', 'GEANT'),
            (r'\bGROSSE?S?\b', 'GROS'),
            (r'\bPETITS?\b', 'PETIT'),
            (r'\bMOYENS?\b', 'MOYEN'),
        ]
        for pattern, calibre_val in calibre_keywords:
            if re.search(pattern, product_upper):
                calibre_trouve = calibre_val
                break

    if calibre_trouve:
        result["Calibre"] = calibre_trouve
        infos_trouvees.append(f"Calibre:{calibre_trouve}")

    # --- Infos brutes (tous les attributs trouvés) ---
    if infos_trouvees:
        result["Infos_Brutes"] = " | ".join(infos_trouvees)

    return result


def extract_data_from_pdf(file_bytes: bytes) -> list[dict]:
    """
    Extrait les données d'un fichier PDF Hennequin.

    Args:
        file_bytes: Contenu binaire du fichier PDF

    Returns:
        Liste de dictionnaires avec les produits extraits
    """
    # 1. Extraction brute des lignes avec coordonnées
    doc = fitz.open(stream=file_bytes, filetype="pdf")
    all_lines = []
    for page_num in range(doc.page_count):
        page = doc[page_num]
        words = page.get_text("words")
        lines_dict = defaultdict(list)
        for w in words:
            x0, y0, x1, y1, text, block_no, line_no, word_no = w
            lines_dict[(block_no, line_no)].append((x0, y0, x1, y1, text))
        for (block_no, line_no), line_words in lines_dict.items():
            if not line_words:
                continue
            line_text = ' '.join([w[-1] for w in line_words]).strip()
            if not line_text:
                continue
            x_first = min([w[0] for w in line_words])
            y_avg = np.mean([w[1] + (w[3] - w[1]) / 2 for w in line_words])
            entry = {
                'page': page_num,
                'x_first': x_first,
                'y_avg': y_avg,
                'text': line_text,
            }
            all_lines.append(entry)
    df_lines = pd.DataFrame(all_lines)

    # 2. Typage des lignes
    def classify_line(row):
        x = row['x_first']
        y = row['y_avg']
        if y > 735:
            return "footer"
        if y < 100:
            if 40 < y < 42:
                return "date"
            else:
                return "header"
        if 19 <= x <= 20 or 300 <= x <= 305:
            return "categorie"
        if 30 <= x <= 33 or 314 <= x <= 316:
            return "produit"
        if 43 <= x <= 46 or 327 <= x <= 332:
            return "qualite"
        if 256 <= x <= 264 or 539 <= x <= 550:
            return "prix"
        return "MAV"
    df_lines['type'] = df_lines.apply(classify_line, axis=1)

    # 3. Parse la date
    date_line = df_lines[df_lines['type'] == 'date']['text']
    if not date_line.empty:
        date_str = date_line.iloc[0]
        m = re.search(r'(\d{2}/\d{2}/\d{4})', date_str)
        if m:
            pricedate_str = m.group(1)
            pricedate = datetime.strptime(pricedate_str, "%d/%m/%Y").date()
        else:
            pricedate = None
    else:
        pricedate = None

    # 4. Filtrer les lignes utiles
    df_lines_filtered = df_lines[~df_lines['type'].isin(['header', 'footer', 'MAV', 'date'])].copy()
    df_lines_filtered['qualite_calibre'] = ""

    # 5. Affecter la qualité/calibre au dernier produit précédent
    last_produit_idx = None
    for idx, row in df_lines_filtered.iterrows():
        t = row['type']
        txt = row['text']
        if t == 'produit':
            last_produit_idx = idx
        elif t == 'qualite' and last_produit_idx is not None:
            prev = df_lines_filtered.at[last_produit_idx, 'qualite_calibre']
            df_lines_filtered.at[last_produit_idx, 'qualite_calibre'] = (prev + " / " if prev else "") + txt

    # 6. Fusionner les blocs de produits consécutifs
    def merge_consecutive_products_with_qualite(df):
        df = df.copy()
        indices_to_drop = []
        n = len(df)
        i = 0
        while i < n:
            if df.iloc[i]['type'] == 'produit':
                start = i
                qualites = []
                while i+1 < n and df.iloc[i+1]['type'] == 'produit':
                    if 'qualite_calibre' in df.columns and pd.notnull(df.iloc[i+1].get('qualite_calibre', None)):
                        qualites.append(df.iloc[i+1]['qualite_calibre'])
                    i += 1
                end = i
                merged_text = " ".join(df.iloc[k]['text'] for k in range(start, end+1))
                df.at[df.index[start], 'text'] = merged_text
                all_qualites = []
                if 'qualite_calibre' in df.columns and pd.notnull(df.iloc[start].get('qualite_calibre', None)):
                    all_qualites.append(df.iloc[start]['qualite_calibre'])
                all_qualites.extend(qualites)
                qualite_str = " ".join(str(q) for q in all_qualites if pd.notnull(q) and q)
                if qualite_str:
                    df.at[df.index[start], 'qualite_calibre'] = qualite_str
                else:
                    df.at[df.index[start], 'qualite_calibre'] = ""
                indices_to_drop.extend(df.index[k] for k in range(start+1, end+1))
            i += 1
        df_merged = df.drop(indices_to_drop)
        return df_merged

    df_intermediate = merge_consecutive_products_with_qualite(df_lines_filtered)

    # 7. Construit le tableau final (catégorie, produit, qualité/calibre, prix, page)
    entries = []
    current_categorie = ''
    last_produit = None
    last_qualite = ''
    current_page = None

    for idx, row in df_intermediate.iterrows():
        t = row['type']
        txt = row['text']
        p = row['page']
        qualite = row['qualite_calibre']
        if t == 'categorie':
            current_categorie = txt
            current_page = p
            last_produit = None
        elif t == 'produit':
            last_produit = txt
            last_qualite = qualite
        elif t == 'prix':
            if last_produit is not None:
                entries.append({
                    'Date': pricedate,
                    'page': p,
                    'Catégorie': current_categorie,
                    'Produit': last_produit,
                    'qualite_calibre': last_qualite,
                    'Prix': txt.replace(',', '.').replace(' ', ''),
                })
                last_produit = None
                last_qualite = ''
    df_final = pd.DataFrame(entries)

    # 8. Nettoyage des noms & mapping des catégories
    df_final['ProductName'] = df_final["Produit"].str.replace(r'\.*$', '', regex=True)
    df_final['ProductName'] = df_final.apply(
        lambda row: row['ProductName'] + " " + row['qualite_calibre'] if pd.notnull(row['qualite_calibre']) and str(row['qualite_calibre']).strip() != "" else row['ProductName'],
        axis=1
    ).str.strip()

    cat_map = {
        'BAR PETIT BATEAU': 'BAR',
        'BAR LIGNE': 'BAR',
        'DORADE ROYALE': 'DORADE',
        'DORADE SAR': 'DORADE',
        'DORADE GRISE': 'DORADE GRISE'  # Garder DORADE GRISE distincte
    }
    df_final['Categorie'] = df_final['Catégorie'].replace(cat_map)

    # 8b. Enrichissement: extraction des attributs depuis ProductName et Categorie
    def enrich_row(row):
        attrs = parse_hennequin_attributes(
            product_name=row.get("ProductName"),
            categorie=row.get("Categorie")
        )
        return pd.Series(attrs)

    enriched = df_final.apply(enrich_row, axis=1)
    df_final = pd.concat([df_final, enriched], axis=1)
    logger.info(f"Hennequin enrichissement: {enriched['Methode_Peche'].notna().sum()} méthodes, "
                f"{enriched['Qualite'].notna().sum()} qualités, {enriched['Origine'].notna().sum()} origines")

    # 9. Ajout Vendor, Code_Provider, keyDate
    df_final['Vendor'] = 'Hennequin'
    df_final['Code_Provider'] = 'HNQ_' + df_final['ProductName'].str.replace(' ', '_', regex=False).str.lower()
    df_final['keyDate'] = df_final['Code_Provider'] + df_final['Date'].apply(lambda d: f"_{d:%y%m%d}" if pd.notnull(d) else "")

    # 10. Sélection des colonnes finales, nettoyage pour JSON
    df_final = df_final[['Date', 'Vendor', "keyDate", 'Code_Provider', 'Prix', 'ProductName', "Categorie",
                         'Methode_Peche', 'Qualite', 'Decoupe', 'Etat', 'Conservation', 'Origine', 'Calibre', 'Infos_Brutes']]
    df_final.replace([np.inf, -np.inf], None, inplace=True)
    df_final = df_final.where(pd.notnull(df_final), None)

    if 'Date' in df_final.columns:
        df_final['Date'] = df_final['Date'].astype(str)

    try:
        json_ready = sanitize_for_json(df_final)
        logger.info("Conversion JSON OK")
        return json_ready
    except Exception:
        logger.exception("Erreur lors du `sanitize_for_json`")
        raise


def parse(file_bytes: bytes, harmonize: bool = False, **kwargs) -> list[dict]:
    """
    Point d'entrée principal du parser Hennequin.

    Extrait les produits d'un PDF Hennequin et optionnellement
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
        - Conservation (spécifique Hennequin)
        - Infos_Brutes (concaténation des attributs extraits)

        Si harmonize=True, les clés sont en snake_case et les valeurs normalisées:
        - categorie (ST PIERRE → SAINT PIERRE)
        - methode_peche (PT BATEAU → PB)
        - qualite (QUALITE PREMIUM → PREMIUM)
        - etat (VIDEE → VIDE, CORAILLEES → CORAILLE)
        - type_production (extrait de SAUVAGE)
        - conservation (CONGELEE → CONGELE)

    Example:
        >>> with open("cours_hennequin.pdf", "rb") as f:
        ...     products = parse(f.read(), harmonize=True)
        >>> products[0]["methode_peche"]
        'PB'  # Normalisé depuis 'PT BATEAU'
    """
    # Extraction des données brutes
    products = extract_data_from_pdf(file_bytes)

    # Affinage des catégories génériques vers espèces spécifiques
    for product in products:
        product["Categorie"] = refine_generic_category(
            product.get("Categorie"),
            product.get("ProductName"),
            HENNEQUIN_GENERIC_CATEGORIES
        )

    # Application optionnelle de l'harmonisation
    if harmonize:
        if not HARMONIZE_AVAILABLE:
            raise ImportError(
                "Le module services.harmonize n'est pas disponible. "
                "Vérifiez que services/harmonize.py existe."
            )
        products = harmonize_products(products, vendor="Hennequin")

    return products


# Alias pour compatibilité
parse_hennequin = parse
