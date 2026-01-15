from fastapi import FastAPI, UploadFile, File, Header, HTTPException, Request, APIRouter, Query, BackgroundTasks
from fastapi.staticfiles import StaticFiles
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.templating import Jinja2Templates
import fitz  # PyMuPDF
import pandas as pd
import numpy as np
import re
import datetime
from datetime import date, datetime
import json
from pydantic import BaseModel
from typing import Optional, List
from google.cloud import bigquery
from google.cloud.bigquery import Client
from google.cloud import bigquery, secretmanager
from google.oauth2 import service_account
import logging
import os


from google.oauth2 import service_account
from fastapi.middleware.cors import CORSMiddleware
import tempfile
from openpyxl import load_workbook
import warnings
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

LOG_DIR = "./logs"
LOG_PATH = os.path.join(LOG_DIR, "pdf_parser.log")
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Nettoyage des handlers
if logger.hasHandlers():
    logger.handlers.clear()

class SafeConsoleFormatter(logging.Formatter):
    def format(self, record):
        msg = super().format(record)
        # Supprime les caractères non imprimables (comme les surrogates / emojis)
        return re.sub(r'[\ud800-\udfff]', '', msg)

# 📄 File handler UTF-8
fh = logging.FileHandler(LOG_PATH, encoding="utf-8")
fh.setLevel(logging.INFO)
fh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

# 🖥️ Console handler sans emojis
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
ch.setFormatter(SafeConsoleFormatter('%(asctime)s - %(levelname)s - %(message)s'))

# Enregistrement des handlers
logger.addHandler(fh)
logger.addHandler(ch)

# Import depuis config.py pour éviter les imports circulaires
import config

def get_secret(secret_id: str) -> str:
    return config.get_secret(secret_id)

def get_api_key():
    return config.get_api_key()

def get_credentials_from_secret_json(secret_name: str, scopes: list = []) -> service_account.Credentials:
    raw = get_secret(secret_name)
    data = json.loads(raw)
    return service_account.Credentials.from_service_account_info(data, scopes=scopes)


def get_bigquery_client(secret_name: str = "providersparser", scopes: list = ["https://www.googleapis.com/auth/cloud-platform","https://www.googleapis.com/auth/drive.readonly"]) -> bigquery.Client:
    """
    Retourne un client BigQuery basé sur les credentials par défaut (GOOGLE_APPLICATION_CREDENTIALS)
    ou Secret Manager en fallback.
    """
    from google.auth import default as get_default_credentials
    import os
    
    try:
        # Essayer d'abord avec les credentials par défaut (Docker)
        credentials, project_id = get_default_credentials(scopes=scopes)
        # Si project_id n'est pas déterminé, utiliser GCP_PROJECT_ID ou config
        if not project_id:
            project_id = os.environ.get("GCP_PROJECT_ID") or config.get_project_id_from_credentials()
        if not project_id:
            raise ValueError("Project ID non déterminé")
        return bigquery.Client(credentials=credentials, project=project_id)
    except Exception as e1:
        # Fallback: essayer avec Secret Manager si disponible
        try:
            credentials_json = get_secret(secret_name)
            info = json.loads(credentials_json)
            credentials = service_account.Credentials.from_service_account_info(info, scopes=scopes)
            return bigquery.Client(credentials=credentials, project=info["project_id"])
        except Exception as e2:
            raise Exception(f"Impossible d'obtenir les credentials BigQuery. Default: {e1}, Secret Manager: {e2}")

class ProductItem(BaseModel):
    keyDate: str
    Vendor: str
    ProductName: str
    Code_Provider: str
    Date: str
    Prix: Optional[float] = None
    Categorie: Optional[str] = None
    
def insert_prices_to_bigquery(data: List[ProductItem], table_id: str = "beo-erp.ERPTables.ProvidersPrices"):
    client = get_bigquery_client()
    dataset_id, table_name = table_id.split(".")[1:]
    temp_table_id = f"{client.project}.{dataset_id}._temp_upload"

    # Convertir les données en DataFrame
    df = pd.DataFrame([row.dict() for row in data])
    df["Date"] = pd.to_datetime(df["Date"], format="%Y-%m-%d", errors="coerce").dt.date

    df = df.drop_duplicates(subset=["keyDate"], keep="last")

    # Upload temporaire
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    job = client.load_table_from_dataframe(df, temp_table_id, job_config=job_config)
    job.result()

    # MERGE sur keyDate
    merge_query = f"""
    MERGE `{table_id}` T
    USING `{temp_table_id}` S
    ON T.keyDate = S.keyDate
    WHEN MATCHED THEN UPDATE SET
      T.Vendor = S.Vendor,
      T.ProductName = S.ProductName,
      T.Code_Provider = S.Code_Provider,
      T.Date = S.Date,
      T.Prix = S.Prix,
      T.Categorie = S.Categorie
    WHEN NOT MATCHED THEN
      INSERT (keyDate, Vendor, ProductName, Code_Provider, Date, Prix, Categorie)
      VALUES (S.keyDate, S.Vendor, S.ProductName, S.Code_Provider, S.Date, S.Prix, S.Categorie)
    """
    client.query(merge_query).result()



def sanitize_for_json(df: pd.DataFrame) -> list[dict]:
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
    return re.match(r"^-?$|^\d+(?:[.,]\d+)?$", val) is not None


# Note: extract_LD_data_from_pdf2 supprimé (code mort, remplacé par extract_LD_data_from_pdf)

from io import BytesIO


def parse_laurent_daniel_attributes(product_name: str, categorie: str = None) -> dict:
    """
    Extrait les attributs structurés depuis ProductName pour Laurent Daniel.
    Inspiré de parse_audierne_attributes() et parse_hennequin_attributes().

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


def extract_LD_data_from_pdf(file_bytes: bytes):
    """
    Extrait les données produits et la date d'un PDF LD, renvoie une liste JSON-ready.
    Prend en entrée les bytes du fichier PDF.
    """
    # Open PDF from bytes
    doc = fitz.open(stream=BytesIO(file_bytes), filetype="pdf")

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
    raw_lines = []
    words_coords = []
    for page_num, page in enumerate(doc):
        for line in page.get_text().splitlines():
            clean_line = line.strip()
            if clean_line:
                raw_lines.append((page_num, clean_line))
        for w in page.get_text("words"):
            x0, y0, x1, y1, word, *_ = w
            words_coords.append({
                'page': page_num,
                'x0': x0,
                'y0': y0,
                'word': word.strip()
            })
    coords_df = pd.DataFrame(words_coords)
    coords_df = coords_df[(coords_df['y0'] >= 100) & (coords_df['word'].str.upper() != 'EURO/KG')].reset_index(drop=True)

    # -------------- Définition des colonnes et fonctions de matching ---------------
    # max_x0 pour calculs de pourcentage (utilisé uniquement dans is_prix/is_qualite)
    max_x0 = coords_df['x0'].max() if len(coords_df) > 0 else 600

    # COLS en pixels fixes - déterminés empiriquement pour CC2.pdf et fonctionnent avec CC.pdf
    # Ces valeurs correspondent à ~30% et ~65% de la largeur page
    COLS = [
        {'name': 'col1', 'x_min': 0,   'x_max': 190},
        {'name': 'col2', 'x_min': 191, 'x_max': 340},
        {'name': 'col3', 'x_min': 341, 'x_max': 600},
    ]

    def is_prix(x0, col_idx):
        # Détecte la position horizontale des prix (avant les qualités)
        # Basé sur calibration CC2.pdf en pixels: col1=133-150, col2=281-299, col3=401-460
        # En pourcentages: col1=27-31%, col2=58-62%, col3=84-96%
        pct = 100 * x0 / max_x0 if max_x0 > 0 else 0
        if col_idx == 0: return 25 <= pct <= 31    # Col1: before qualite (31+)
        if col_idx == 1: return 55 <= pct <= 62    # Col2: before qualite (62+)
        if col_idx == 2: return 75 <= pct <= 96    # Col3: before qualite (96+)
        return False

    def is_qualite(x0, col_idx):
        # Détecte la position horizontale des qualités/calibres (après les prix)
        # Basé sur calibration CC2.pdf en pixels: col1=151-190, col2=300-340, col3>461
        # En pourcentages: col1=31-40%, col2=62-71%, col3>96%
        pct = 100 * x0 / max_x0 if max_x0 > 0 else 0
        if col_idx == 0: return pct >= 31          # Col1: from 31% onwards
        if col_idx == 1: return pct >= 62          # Col2: from 62% onwards
        if col_idx == 2: return pct >= 96          # Col3: from 96% onwards
        return False

    def is_categorie(words, x0s, col_idx):
        # Catégories : basées sur pourcentage de max_x0
        # Original: col1=min>=55, col3=min>=360
        # En % de max_x0 (480): col1=11%, col3=75%
        pcts = [100 * x0 / max_x0 for x0 in x0s] if max_x0 > 0 else x0s
        min_pct = min(pcts) if pcts else 0
        if col_idx == 0:
            return min_pct >= 11  # Col1: au moins 11%
        if col_idx == 2:
            return min_pct >= 75  # Col3: au moins 75%
        return all(w.isupper() for w in words)
    # ------------------------- Extraction des produits -----------------------------
    results = []
    for col_idx, col in enumerate(COLS):
        col_df = coords_df[(coords_df['x0'] >= col['x_min']) & (coords_df['x0'] <= col['x_max'])].copy()
        col_df = col_df.sort_values(['y0', 'x0']).reset_index(drop=True)
        cat = None
        for y0, group in col_df.groupby('y0'):
            words = list(group['word'])
            x0s = list(group['x0'])
            if is_categorie(words, x0s, col_idx):
                cat_words = [w for w in words if w != "-"]
                if cat_words:
                    cat = " ".join(cat_words).strip("- ").strip()
                continue
            produit_mots = []
            prix = ""
            qualite = ""
            for w, x0 in zip(words, x0s):
                pct = 100 * x0 / max_x0 if max_x0 > 0 else 0
                if is_qualite(x0, col_idx):
                    qualite = w
                    # DEBUG
                    # logger.debug(f"COL{col_idx} QUALITE: '{w}' @x0={x0:.0f} ({pct:.1f}%)")
                elif is_prix(x0, col_idx):
                    # Heuristique: si le mot ressemble à un prix (nombre avec virgule/point/tiret), c'est un prix
                    # Sinon c'est probablement une qualité (PB, EXTRA, etc.)
                    is_numeric = bool(re.search(r'\d+[,.\-]?\d*|^-$', w))
                    if is_numeric:
                        prix = w
                        # DEBUG
                        # logger.debug(f"COL{col_idx} PRIX: '{w}' @x0={x0:.0f} ({pct:.1f}%)")
                    else:
                        qualite = w
                        # DEBUG
                        # logger.debug(f"COL{col_idx} QUALITE(numeric): '{w}' @x0={x0:.0f} ({pct:.1f}%)")
                else:
                    produit_mots.append(w)
                    # DEBUG
                    # logger.debug(f"COL{col_idx} PRODUIT: '{w}' @x0={x0:.0f} ({pct:.1f}%)")
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
    )
    # Utiliser to_numeric avec coerce pour éviter les erreurs de conversion
    df_final['Prix'] = pd.to_numeric(df_final['Prix'], errors='coerce')
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
        ('st pierre','SAINT PIERRE')
    ]
    for prefix, cat in rules:
        mask = df_final['produit_lower'].str.startswith(prefix)
        df_final.loc[mask, 'categorie'] = cat
    df_final['Categorie'] = df_final['categorie'].str.upper()
    df_final = df_final.drop(columns=['produit_lower'])
    df_final['Code_Provider'] = 'LD_'+df_final['produit'].str.replace(" ","") + "_" + df_final["qualite"]
    df_final['Date'] = date_str
    df_final['Vendor'] = "LD"
    df_final["keyDate"] = df_final["Code_Provider"] + "_" + str(date_str)
    df_final["ProductName"] = df_final["produit"] + " " + df_final["qualite"]

    # ---------- Enrichissement des attributs depuis ProductName ----------------------
    # Extraction des attributs structurés (Methode_Peche, Calibre, etc.)
    enriched_attributes = []
    for _, row in df_final.iterrows():
        attrs = parse_laurent_daniel_attributes(row["ProductName"], row["Categorie"])
        enriched_attributes.append(attrs)

    attrs_df = pd.DataFrame(enriched_attributes)
    for col in attrs_df.columns:
        df_final[col] = attrs_df[col]

    df_final2 = df_final[['Date','Vendor', "keyDate",'Code_Provider','Prix','ProductName',"Categorie",
                          'Methode_Peche','Qualite','Calibre','Decoupe','Etat','Origine','Infos_Brutes']]

    # ---------- Appel de la fonction de sanitization/JSON + gestion d'erreur -------
    try:
        json_ready = sanitize_for_json(df_final2)
        logger.info("Conversion JSON OK")
        return json_ready
    except Exception:
        logger.exception("Erreur lors du `sanitize_for_json`")
        raise



# === FASTAPI APP ===
app = FastAPI(
    # docs_url=None,
    # redoc_url=None,
    # openapi_url=None
)


app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")


# Permettre au front (même sur un autre domaine) d'accéder à l'API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Sécurisable par domaine plus tard
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============================================================
# Import Services (nouvelle architecture ELT)
# ============================================================
from services.import_service import ImportService

# Wrappers pour les parsers existants
def parse_laurent_daniel(file_bytes: bytes, **kwargs) -> list[dict]:
    """Wrapper pour parser Laurent-Daniel."""
    return extract_LD_data_from_pdf(file_bytes)

def parse_vvqm(file_bytes: bytes, **kwargs) -> list[dict]:
    """Wrapper pour parser VVQM."""
    df = parse_vvq_pdf_data(file_bytes)
    # Le parser retourne déjà un DataFrame avec les bonnes colonnes
    return sanitize_for_json(df[["keyDate", "Vendor", "ProductName", "Code_Provider", "Date", "Prix", "Categorie"]])

def parse_demarne(file_bytes: bytes, date_fallback: Optional[str] = None, **kwargs) -> list[dict]:
    """Wrapper pour parser Demarne."""
    import tempfile
    with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
        tmp.write(file_bytes)
        tmp_path = tmp.name
    return parse_demarne_excel_data(tmp_path, date_fallback=date_fallback)

def parse_hennequin(file_bytes: bytes, **kwargs) -> list[dict]:
    """Wrapper pour parser Hennequin."""
    return extract_hennequin_data_from_pdf(file_bytes)

# Initialiser les services d'import
ld_service = ImportService("laurent_daniel", parse_laurent_daniel)
vvqm_service = ImportService("vvqm", parse_vvqm)
demarne_service = ImportService("demarne", parse_demarne)
hennequin_service = ImportService("hennequin", parse_hennequin)


########################################################################################################################
############################################################ Laurent Daniel ############################################
########################################################################################################################
@app.post("/parseLaurentDpdf")
async def parse_laurent_d_pdf(
    file: UploadFile = File(...),
    background_tasks: BackgroundTasks = BackgroundTasks(),
    x_api_key: str = Header(default=None)
):
    """Nouveau endpoint avec BackgroundTasks et ELT pipeline."""
    if x_api_key != get_api_key():
        raise HTTPException(status_code=403, detail="Invalid API Key")

    file_bytes = await file.read()
    return ld_service.handle_import(file.filename, file_bytes, background_tasks)


@app.post("/testLaurentDpdf")
async def test_laurent_d_pdf(
    file: UploadFile = File(...),
):
    """
    Endpoint simple de TEST pour Laurent Daniel - SANS authentification.
    Retourne les données extraites sans enrichissement pour debug.
    """
    try:
        file_bytes = await file.read()
        data = extract_LD_data_from_pdf(file_bytes)

        # Stats
        total = len(data)
        with_prix = sum(1 for p in data if p.get('Prix') is not None)

        return {
            "status": "success",
            "file": file.filename,
            "total_products": total,
            "products_with_price": with_prix,
            "price_coverage": f"{100*with_prix//total if total > 0 else 0}%",
            "sample_10": data[:10] if data else [],
            "all_data": data
        }
    except Exception as e:
        logger.error(f"Erreur test Laurent Daniel: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/parseLaurentDpdf")
async def parse_pdf_local(x_api_key: str = Header(default=None)):
    if x_api_key != get_api_key():
        raise HTTPException(status_code=403, detail="Invalid API Key")
    path = r"Samples/LaurentD/CC.pdf"
    file_bytes = open(path, "rb").read()
    return extract_LD_data_from_pdf(file_bytes)


########################################################################################################################
############################################################ VVQM  #####################################################
########################################################################################################################
@app.post("/parseVVQpdf")
async def parse_vvq_pdf(
    file: UploadFile = File(...),
    background_tasks: BackgroundTasks = BackgroundTasks(),
    x_api_key: str = Header(default=None)
):
    """Nouveau endpoint avec BackgroundTasks et ELT pipeline."""
    if x_api_key != get_api_key():
        raise HTTPException(status_code=403, detail="Invalid API Key")
    
    file_bytes = await file.read()
    return vvqm_service.handle_import(file.filename, file_bytes, background_tasks)


@app.get("/parseVVQpdf_test")
async def parse_vvq_pdf_test():
    try:
        path = "Samples/VVQ/GEXPORT.pdf"
        with open(path, "rb") as f:
            file_bytes = f.read()

        df = parse_vvq_pdf_data(file_bytes)
        data = sanitize_for_json(df)

        return JSONResponse(content=data)

    except Exception as e:
        logger.exception("Erreur dans /parseVVQpdf_test")
        raise HTTPException(status_code=500, detail=f"Erreur traitement PDF test : {e}")


@app.post("/parseVVQStructured")
async def parse_vvq_structured_endpoint(
    file: UploadFile = File(...),
    load_to_bq: bool = Query(False, description="Charger les donnees dans BigQuery VVQMStructured"),
    x_api_key: str = Header(default=None)
):
    """
    Endpoint pour parser VVQM avec structure complete enrichie.
    Retourne un DataFrame avec toutes les colonnes extraites:
    - Espece, Methode_Peche, Etat, Decoupe, Origine, Section, Calibre
    - Categorie automatique (sans lookup BigQuery CodesNames)
    """
    if x_api_key != get_api_key():
        raise HTTPException(status_code=403, detail="Invalid API Key")

    try:
        file_bytes = await file.read()
        df = parse_vvq_pdf_data(file_bytes)

        # Optionnel: charger dans BigQuery
        if load_to_bq:
            rows_loaded = load_vvqm_structured_to_bigquery(df)
            return {
                "status": "success",
                "rows_parsed": len(df),
                "rows_loaded_to_bq": rows_loaded,
                "sample": sanitize_for_json(df.head(10))
            }

        return {
            "status": "success",
            "rows_parsed": len(df),
            "data": sanitize_for_json(df)
        }

    except Exception as e:
        logger.error(f"Erreur parsing VVQM structured: {e}")
        raise HTTPException(status_code=500, detail=str(e))


########################################################################################################################
############################################################ Demarne  ##################################################
########################################################################################################################


def extract_date_from_excel_header(path, date_fallback: Optional[str] = None):
    """
    Extrait la date depuis les métadonnées Excel (header).
    Si aucune date n'est trouvée, utilise date_fallback si fourni.
    Retourne None seulement si aucune date n'est trouvée ET aucun fallback fourni.
    """
    wb = load_workbook(path, data_only=True)
    sheet = wb.active
    header = sheet.oddHeader.center.text if sheet.oddHeader and sheet.oddHeader.center else ""
    match = re.search(r"\b(\d{2})/(\d{2})/(\d{4})\b", header)
    if match:
        day, month, year = match.groups()
        return date(int(year), int(month), int(day)).isoformat()
    
    # Fallback: utiliser la date fournie par l'utilisateur
    if date_fallback:
        # Valider le format de la date fallback (YYYY-MM-DD)
        try:
            parsed_date = datetime.strptime(date_fallback, "%Y-%m-%d").date()
            return parsed_date.isoformat()
        except ValueError:
            # Essayer aussi le format DD/MM/YYYY
            try:
                parsed_date = datetime.strptime(date_fallback, "%d/%m/%Y").date()
                return parsed_date.isoformat()
            except ValueError:
                raise ValueError(f"Format de date invalide pour le fallback: '{date_fallback}'. Utilisez YYYY-MM-DD ou DD/MM/YYYY")
    
    return None


@app.post("/testDate")
async def testDate(file: UploadFile = File(...)):
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
            tmp.write(await file.read())
            tmp_path = tmp.name
            price_date = extract_date_from_excel_header(tmp_path)
            return(price_date)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur parsing XLS : {e}")
    
    

def parse_demarne_excel_data(excel_path: str, date_fallback: Optional[str] = None):
    price_date = extract_date_from_excel_header(excel_path, date_fallback)
    
    # Validation: la date ne doit jamais être None
    if price_date is None:
        raise ValueError(
            "ERREUR: Date manquante. "
            "Aucune date n'a été trouvée dans les métadonnées du fichier Excel "
            "et aucun paramètre 'date' n'a été fourni dans la requête. "
            "Veuillez fournir le paramètre 'date' au format YYYY-MM-DD ou DD/MM/YYYY "
            "(exemple: date=2024-01-15 ou date=15/01/2024)."
        )
    
    df = pd.read_excel(excel_path, header=2)
    df.columns = df.columns.str.strip()
    df_filtered = df.iloc[:, [3, 6]]  # 4e et 7e colonnes
    df_filtered.columns = ["Code_Provider", "Prix"]
    df_filtered["Code_Provider"] = df_filtered["Code_Provider"].astype(str)
    df_filtered = df_filtered.dropna(subset=["Code_Provider", "Prix"])
    df_filtered = df_filtered[df_filtered["Code_Provider"] != "Code"]

    # 📥 Charger la table CodesNamesDemarne depuis BigQuery
    client = get_bigquery_client()
    codes_names_query = """
        SELECT Code, Name, Categorie
        FROM `lacriee.PROD.CodesNames`
        WHERE Vendor='Demarne'
    """
    codes_names_df = client.query(codes_names_query).result().to_dataframe(create_bqstorage_client=False)
    codes_names_df["Code"] = codes_names_df["Code"].astype(str)

    # 🔗 Faire la jointure
    df_filtered = df_filtered.merge(
        codes_names_df,
        left_on="Code_Provider",
        right_on="Code",
        how="left"
    )

    # 📝 Si pas de correspondance, utiliser le Code_Provider comme fallback
    df_filtered["ProductName"] = df_filtered["Name"].fillna(df_filtered["Code_Provider"])

    # 🎣 Extraire la méthode de pêche depuis ProductName et Categorie
    df_filtered["Methode_Peche"] = df_filtered.apply(
        lambda row: parse_demarne_fishing_method(
            product_name=row.get("ProductName"),
            categorie=row.get("Categorie")
        ),
        axis=1
    )

    # 🗓️ Ajout des colonnes standard
    df_filtered["Date"] = price_date
    df_filtered["Vendor"] = "Demarne"
    df_filtered["keyDate"] = df_filtered["Code_Provider"] + "_" + df_filtered["Date"]

    # Nettoyage pour JSON
    return sanitize_for_json(df_filtered[["keyDate", "Vendor", "ProductName", "Code_Provider", "Date", "Prix", "Categorie", "Methode_Peche"]])


def split_fr_en(text: str):
    """
    Sépare un texte français/anglais séparé par "/"
    Ex: "SAUMON SUPÉRIEUR / SUPERIOR SALMON" -> ("SAUMON SUPÉRIEUR", "SUPERIOR SALMON")
    Ex: "Dorade Royale/ Gilthead bream" -> ("Dorade Royale", "Gilthead bream")
    Ex: "Dorade Royale / Gilthead bream" -> ("Dorade Royale", "Gilthead bream")
    Gère aussi les cas où le "/" est dans un mot (ex: "Trim B/D" ne sera pas split)
    """
    if not text or not isinstance(text, str):
        return text, None

    text = text.strip()

    # Chercher un "/" avec au moins un espace d'un côté, ou "/" suivi d'une majuscule
    # Pattern 1: "texte FR / texte EN" (espaces des deux côtés)
    # Pattern 2: "texte FR/ Texte EN" (espace après seulement, EN commence par majuscule)
    # Pattern 3: "texte FR /texte EN" (espace avant seulement)
    match = re.match(r'^(.+?)\s*/\s*([A-Z].+)$', text)
    if match:
        fr = match.group(1).strip()
        en = match.group(2).strip()
        return fr, en

    # Pattern alternatif: "/" avec espace avant mais pas après, suivi de minuscule
    match = re.match(r'^(.+?)\s+/\s*(.+)$', text)
    if match:
        fr = match.group(1).strip()
        en = match.group(2).strip()
        return fr, en

    return text, None


def parse_demarne_fishing_method(
    product_name: Optional[str] = None,
    categorie: Optional[str] = None,
    variante: Optional[str] = None,
    label: Optional[str] = None
) -> Optional[str]:
    """
    Extrait la méthode de pêche depuis les champs Demarne.
    
    Priorité de recherche : Variante > Label > Categorie > ProductName
    
    Args:
        product_name: Nom complet du produit
        categorie: Catégorie du produit (ex: "SAUMON SUPÉRIEUR NORVÈGE")
        variante: Variante du produit (ex: "Ligne", "Entier")
        label: Label/certification (ex: "MSC", "BIO")
    
    Returns:
        Méthode de pêche extraite (ex: "LIGNE", "PB", "IKEJIME") ou None
    """
    # Mots-clés de méthode de pêche à rechercher
    # Ordre important : patterns plus longs d'abord pour éviter les faux positifs
    fishing_methods = [
        ("DE LIGNE", "LIGNE"),
        ("LIGNE", "LIGNE"),
        ("IKEJIME", "IKEJIME"),
        ("IKE", "IKEJIME"),
        ("PB", "PB"),  # Petite Bouche / Pêche à la Bolinche
        ("CASIER", "CASIER"),
        ("FILET", None),  # Exclure FILET car c'est une découpe, pas une méthode
        ("CHALUT", "CHALUT"),
        ("PALANGRE", "PALANGRE"),
        ("FILEYEUR", "FILEYEUR"),
    ]
    
    def extract_from_text(text: str) -> Optional[str]:
        """Cherche une méthode de pêche dans un texte."""
        if not text:
            return None
        text_upper = text.upper().strip()
        
        for pattern, method in fishing_methods:
            if method is None:
                continue  # Patterns à ignorer
            # Chercher le pattern comme mot entier (pas dans un autre mot)
            if re.search(rf'\b{pattern}\b', text_upper):
                return method
        return None
    
    # Chercher dans l'ordre de priorité
    # 1. Variante (plus susceptible de contenir "Ligne", etc.)
    result = extract_from_text(variante)
    if result:
        return result
    
    # 2. Label (peut contenir des certifications de pêche)
    result = extract_from_text(label)
    if result:
        return result
    
    # 3. Categorie
    result = extract_from_text(categorie)
    if result:
        return result
    
    # 4. ProductName en dernier recours
    result = extract_from_text(product_name)
    if result:
        return result
    
    return None


def propagate_variante_translations(df: pd.DataFrame) -> pd.DataFrame:
    """
    Propage les traductions anglaises des variantes.
    Pour chaque Variante FR sans traduction EN, cherche si une autre ligne
    a la même Variante FR avec une traduction EN et la copie.
    """
    # Construire un dictionnaire des traductions connues: Variante FR -> Variante EN
    translations = {}
    for _, row in df.iterrows():
        variante_fr = row.get("Variante")
        variante_en = row.get("Variante_EN")
        if variante_fr and variante_en:
            translations[variante_fr] = variante_en

    # Propager les traductions
    if translations:
        def fill_variante_en(row):
            if row["Variante_EN"] is None and row["Variante"] in translations:
                return translations[row["Variante"]]
            return row["Variante_EN"]

        df["Variante_EN"] = df.apply(fill_variante_en, axis=1)
        logger.info(f"Demarne: {len(translations)} traductions de variantes propagées")

    return df


def get_merged_cells_map(sheet):
    """
    Construit un dictionnaire des cellules fusionnées.
    Pour chaque cellule (row, col) dans une plage fusionnée,
    retourne la valeur de la cellule en haut à gauche de la plage.

    Returns:
        dict: {(row, col): value} pour toutes les cellules dans des plages fusionnées
    """
    merged_map = {}

    for merged_range in sheet.merged_cells.ranges:
        # Valeur de la cellule en haut à gauche
        top_left_value = sheet.cell(row=merged_range.min_row, column=merged_range.min_col).value

        # Propager cette valeur à toutes les cellules de la plage
        for row in range(merged_range.min_row, merged_range.max_row + 1):
            for col in range(merged_range.min_col, merged_range.max_col + 1):
                merged_map[(row, col)] = top_left_value

    return merged_map


def parse_demarne_structured(excel_path: str, date_fallback: Optional[str] = None):
    """
    Parse le fichier Excel Demarne avec extraction structurée de toutes les colonnes.
    Gère les cellules fusionnées (notamment Calibre et Origine).
    Retourne un DataFrame avec colonnes: Code, Categorie, Categorie_EN, Variante,
    Variante_EN, Label, Calibre, Origine, Colisage, Tarif, Unite_Facturee, Date, Vendor
    """
    price_date = extract_date_from_excel_header(excel_path, date_fallback)

    if price_date is None:
        raise ValueError(
            "ERREUR: Date manquante. "
            "Aucune date n'a été trouvée dans les métadonnées du fichier Excel "
            "et aucun paramètre 'date' n'a été fourni dans la requête."
        )

    # Charger avec openpyxl pour détecter les cellules fusionnées
    wb = load_workbook(excel_path, data_only=True)
    sheet = wb.active

    # Construire la map des cellules fusionnées
    merged_map = get_merged_cells_map(sheet)
    logger.info(f"Demarne: {len(merged_map)} cellules dans des plages fusionnées détectées")

    # Lire aussi avec pandas pour faciliter l'itération
    df_raw = pd.read_excel(excel_path, header=None)

    # Variables de contexte pour héritage (Catégorie et Variante seulement)
    current_categorie = None
    current_categorie_en = None
    current_variante = None
    current_variante_en = None

    # Séparateurs de section à ignorer
    separators = ["LA MARÉE", "LA MAREE", "LES COQUILLAGES"]

    entries = []

    for idx, row in df_raw.iterrows():
        excel_row = idx + 1  # Excel est 1-indexed

        # Fonction helper pour obtenir la valeur (fusionnée ou non)
        def get_cell_value(col_idx):
            excel_col = col_idx + 1  # Excel est 1-indexed
            # Vérifier si la cellule est dans une plage fusionnée
            if (excel_row, excel_col) in merged_map:
                return merged_map[(excel_row, excel_col)]
            # Sinon, utiliser la valeur pandas (None si vide)
            val = row[col_idx]
            return val if pd.notna(val) else None

        # Récupérer les valeurs avec gestion des fusions
        col0_val = get_cell_value(0)
        col1_val = get_cell_value(1)
        col2_val = get_cell_value(2)  # Calibre - peut être fusionné
        col3_val = get_cell_value(3)  # Code
        col4_val = get_cell_value(4)  # Origine - peut être fusionné
        col5_val = get_cell_value(5)  # Colisage
        col6_val = get_cell_value(6)  # Tarif
        col7_val = get_cell_value(7)  # Unité

        # Convertir en strings nettoyées
        col0 = str(col0_val).strip() if col0_val is not None else ""
        col1 = str(col1_val).strip() if col1_val is not None else ""
        col2 = str(col2_val).strip() if col2_val is not None else ""
        col3 = col3_val  # Garder tel quel pour vérifier nan
        col4 = str(col4_val).strip() if col4_val is not None else ""
        col5 = str(col5_val).strip() if col5_val is not None else ""
        col6 = col6_val  # Garder numérique
        col7 = str(col7_val).strip() if col7_val is not None else ""

        # Détecter ligne d'en-tête de paragraphe (contient "Code" ou "Calibre" ou "Caliber" en col 3)
        if col3 in ['Code', 'Calibre', 'Caliber']:
            if col0 and col0 not in separators:
                current_categorie, current_categorie_en = split_fr_en(col0)
                current_variante = None
                current_variante_en = None
            continue

        # Ignorer lignes sans code valide
        if col3 is None:
            continue

        # Vérifier si col3 est un code valide (doit être numérique)
        try:
            code_str = str(col3).strip()
            if code_str in ["", "nan"]:
                continue
            float(code_str)  # Les codes Demarne sont numériques
        except ValueError:
            # Ce n'est pas un code valide, ignorer (probablement un séparateur)
            continue

        # Détecter nouvelle variante (col 0 non vide et ce n'est pas un séparateur)
        if col0 and col0 != "nan" and col0 not in separators:
            current_variante, current_variante_en = split_fr_en(col0)

        # Construire le nom complet du produit
        name_parts = []
        if current_categorie:
            name_parts.append(current_categorie)
        if current_variante:
            name_parts.append(current_variante)
        if col1:  # Label
            name_parts.append(col1)
        if col2:  # Calibre
            name_parts.append(col2)

        product_name = " - ".join(filter(None, name_parts))

        # Convertir le tarif de manière sécurisée
        tarif = None
        if col6 is not None:
            if isinstance(col6, (int, float)) and not pd.isna(col6):
                tarif = float(col6)
            elif isinstance(col6, str):
                try:
                    tarif = float(col6)
                except ValueError:
                    pass

        # Extraire la méthode de pêche
        methode_peche = parse_demarne_fishing_method(
            product_name=product_name,
            categorie=current_categorie,
            variante=current_variante,
            label=col1 if col1 else None
        )

        # Extraire les données
        entry = {
            "Code": code_str,
            "Categorie": current_categorie,
            "Categorie_EN": current_categorie_en,
            "Variante": current_variante,
            "Variante_EN": current_variante_en,
            "Methode_Peche": methode_peche,
            "Label": col1 if col1 else None,
            "Calibre": col2 if col2 else None,
            "Origine": col4 if col4 else None,
            "Colisage": col5 if col5 else None,
            "Tarif": tarif,
            "Unite_Facturee": col7 if col7 else None,
            "ProductName": product_name,
            "Date": price_date,
            "Vendor": "Demarne"
        }
        entries.append(entry)

    # Construire DataFrame
    df = pd.DataFrame(entries)

    # Propager les traductions anglaises des variantes
    df = propagate_variante_translations(df)

    # Ajouter keyDate
    df["keyDate"] = df["Code"] + "_" + df["Date"]
    df["Code_Provider"] = df["Code"]  # Alias pour compatibilité
    df["Prix"] = df["Tarif"]  # Alias pour compatibilité

    logger.info(f"Demarne structured parsing: {len(df)} lignes extraites")

    return df


def get_lacriee_bigquery_client():
    """
    Retourne un client BigQuery pour le projet lacriee.
    Utilise lacrieeparseur.json en local, ou les credentials par défaut en production.
    """
    import os
    from pathlib import Path

    # Chercher lacrieeparseur.json en local
    local_creds_path = Path(__file__).parent / "config" / "lacrieeparseur.json"

    if local_creds_path.exists():
        with open(local_creds_path, 'r') as f:
            creds_info = json.load(f)
        credentials = service_account.Credentials.from_service_account_info(creds_info)
        return bigquery.Client(credentials=credentials, project=creds_info['project_id'])

    # Fallback: utiliser les credentials par défaut (pour Cloud Run)
    return bigquery.Client(project="lacriee")


def load_demarne_structured_to_bigquery(df: pd.DataFrame, table_id: str = "lacriee.PROD.DemarneStructured"):
    """
    Charge le DataFrame Demarne structuré dans une table BigQuery dédiée.
    """
    client = get_lacriee_bigquery_client()

    # Configuration du job
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema=[
            bigquery.SchemaField("keyDate", "STRING"),
            bigquery.SchemaField("Code", "STRING"),
            bigquery.SchemaField("Code_Provider", "STRING"),
            bigquery.SchemaField("Categorie", "STRING"),
            bigquery.SchemaField("Categorie_EN", "STRING"),
            bigquery.SchemaField("Variante", "STRING"),
            bigquery.SchemaField("Variante_EN", "STRING"),
            bigquery.SchemaField("Methode_Peche", "STRING"),
            bigquery.SchemaField("Label", "STRING"),
            bigquery.SchemaField("Calibre", "STRING"),
            bigquery.SchemaField("Origine", "STRING"),
            bigquery.SchemaField("Colisage", "STRING"),
            bigquery.SchemaField("Tarif", "FLOAT64"),
            bigquery.SchemaField("Prix", "FLOAT64"),
            bigquery.SchemaField("Unite_Facturee", "STRING"),
            bigquery.SchemaField("ProductName", "STRING"),
            bigquery.SchemaField("Date", "STRING"),
            bigquery.SchemaField("Vendor", "STRING"),
        ]
    )

    # Charger
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()  # Attendre la fin

    logger.info(f"Chargé {len(df)} lignes dans {table_id}")
    return len(df)


def load_hennequin_structured_to_bigquery(df: pd.DataFrame, table_id: str = "lacriee.PROD.HennequinStructured"):
    """
    Charge le DataFrame Hennequin structuré dans une table BigQuery dédiée.
    """
    client = get_lacriee_bigquery_client()
    
    # S'assurer que Date est au format DATE pour BigQuery
    df = df.copy()
    if 'Date' in df.columns:
        # Convertir en datetime puis en date
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce').dt.date
    
    # Configuration du job
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema=[
            bigquery.SchemaField("keyDate", "STRING"),
            bigquery.SchemaField("Code_Provider", "STRING"),
            bigquery.SchemaField("ProductName", "STRING"),
            bigquery.SchemaField("Categorie", "STRING"),
            bigquery.SchemaField("Methode_Peche", "STRING"),
            bigquery.SchemaField("Qualite", "STRING"),
            bigquery.SchemaField("Decoupe", "STRING"),
            bigquery.SchemaField("Etat", "STRING"),
            bigquery.SchemaField("Conservation", "STRING"),
            bigquery.SchemaField("Origine", "STRING"),
            bigquery.SchemaField("Calibre", "STRING"),
            bigquery.SchemaField("Infos_Brutes", "STRING"),
            bigquery.SchemaField("Prix", "FLOAT64"),
            bigquery.SchemaField("Date", "DATE", mode="NULLABLE"),
            bigquery.SchemaField("Vendor", "STRING"),
        ]
    )
    
    # Charger
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()  # Attendre la fin
    
    logger.info(f"Chargé {len(df)} lignes dans {table_id}")
    return len(df)


@app.post("/parseDemarneStructured")
async def parse_demarne_structured_endpoint(
    file: UploadFile = File(...),
    date: Optional[str] = Query(
        None,
        description="Date de fallback au format YYYY-MM-DD ou DD/MM/YYYY.",
        example="2024-01-15"
    ),
    load_to_bq: bool = Query(False, description="Charger les données dans BigQuery"),
    x_api_key: str = Header(default=None)
):
    """
    Nouveau endpoint pour parser Demarne avec structure complète.
    Retourne un DataFrame avec toutes les colonnes extraites.
    """
    if x_api_key != get_api_key():
        raise HTTPException(status_code=403, detail="Invalid API Key")

    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
            tmp.write(await file.read())
            tmp_path = tmp.name

        df = parse_demarne_structured(tmp_path, date_fallback=date)

        # Optionnel: charger dans BigQuery
        if load_to_bq:
            rows_loaded = load_demarne_structured_to_bigquery(df)
            return {
                "status": "success",
                "rows_parsed": len(df),
                "rows_loaded_to_bq": rows_loaded,
                "sample": sanitize_for_json(df.head(10).to_dict(orient="records"))
            }

        return {
            "status": "success",
            "rows_parsed": len(df),
            "data": sanitize_for_json(df.to_dict(orient="records"))
        }

    except Exception as e:
        logger.error(f"Erreur parsing Demarne structured: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if 'tmp_path' in locals():
            os.unlink(tmp_path)


@app.post("/parseDemarneXLS")
async def parse_demarne_xls(
    file: UploadFile = File(...),
    background_tasks: BackgroundTasks = BackgroundTasks(),
    date: Optional[str] = Query(
        None,
        description="Date de fallback au format YYYY-MM-DD ou DD/MM/YYYY. "
                   "Obligatoire si la date n'est pas présente dans les métadonnées du fichier Excel. "
                   "Exemples: '2024-01-15' ou '15/01/2024'",
        example="2024-01-15"
    ),
    x_api_key: str = Header(default=None)
):
    """Nouveau endpoint avec BackgroundTasks et ELT pipeline."""
    if x_api_key != get_api_key():
        raise HTTPException(status_code=403, detail="Invalid API Key")
    
    file_bytes = await file.read()
    return demarne_service.handle_import(
        file.filename, file_bytes, background_tasks,
        parser_kwargs={"date_fallback": date}
    )

@app.get("/parseDemarneXLS")
def parse_demarne_xls_local(x_api_key: str = Header(default=None)):
    if x_api_key != get_api_key():
        raise HTTPException(status_code=403, detail="Invalid API Key")
    try:
        excel_path = "Samples/Demarne/Classeur2.xlsx"
        result = parse_demarne_excel_data(excel_path)
        return JSONResponse(content=result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur parsing XLS local : {e}")



########################################################################################################################
############################################################ VVQM  ###########################################################
########################################################################################################################


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
    # Ordre important: les patterns plus longs/spécifiques doivent être en premier
    # Attention: BARBUE contient "BAR", donc doit être vérifié avant
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


def parse_vvq_pdf_data(file_bytes: bytes) -> pd.DataFrame:
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
    # Les sections s'appliquent uniquement aux produits de la colonne de droite
    COLONNE_4_X_MIN = 500

    def get_section_for_y(y_pos, x_price):
        """
        Retourne la section pour une position Y donnée.
        Les sections s'appliquent UNIQUEMENT aux produits de la colonne 4 (x > 500).
        """
        # Si le prix n'est pas dans la colonne 4, pas de section
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
    df_final["Code_Provider"] = df_final["Code_Provider"].str.replace("__","_")

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

    # Catégorisation automatique (sans BigQuery CodesNames)
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


def load_vvqm_structured_to_bigquery(df: pd.DataFrame, table_id: str = "lacriee.PROD.VVQMStructured"):
    """
    Charge le DataFrame VVQM structure dans une table BigQuery dediee.
    """
    client = get_lacriee_bigquery_client()

    # Preparer le DataFrame pour BigQuery
    df_bq = df.copy()
    df_bq["Code"] = df_bq["Code_Provider"]

    # Configuration du job
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema=[
            bigquery.SchemaField("keyDate", "STRING"),
            bigquery.SchemaField("Code", "STRING"),
            bigquery.SchemaField("Code_Provider", "STRING"),
            bigquery.SchemaField("Espece", "STRING"),
            bigquery.SchemaField("Methode_Peche", "STRING"),
            bigquery.SchemaField("Etat", "STRING"),
            bigquery.SchemaField("Decoupe", "STRING"),
            bigquery.SchemaField("Origine", "STRING"),
            bigquery.SchemaField("Section", "STRING"),
            bigquery.SchemaField("Calibre", "STRING"),
            bigquery.SchemaField("Prix", "FLOAT64"),
            bigquery.SchemaField("Categorie", "STRING"),
            bigquery.SchemaField("ProductName", "STRING"),
            bigquery.SchemaField("Date", "STRING"),
            bigquery.SchemaField("Vendor", "STRING"),
        ]
    )

    # Selectionner les colonnes dans l'ordre du schema
    cols_to_load = [
        "keyDate", "Code", "Code_Provider", "Espece", "Methode_Peche",
        "Etat", "Decoupe", "Origine", "Section", "Calibre",
        "Prix", "Categorie", "ProductName", "Date", "Vendor"
    ]
    df_bq = df_bq[cols_to_load]

    # Convertir Prix en float
    df_bq["Prix"] = pd.to_numeric(df_bq["Prix"], errors="coerce")

    # Charger
    job = client.load_table_from_dataframe(df_bq, table_id, job_config=job_config)
    job.result()  # Attendre la fin

    logger.info(f"Charge {len(df_bq)} lignes dans {table_id}")
    return len(df_bq)


# Note: test_vvq_parser supprime (code mort de developpement)
# Note: imports dupliques supprimes (fitz, pandas, numpy, re, datetime, io deja importes en haut)

from collections import defaultdict


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


def extract_hennequin_data_from_pdf(file_bytes):
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
        'DORADE SAR': 'DORADE'
        # ... ajoute d'autres mappings si besoin !
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


def parse_hennequin_structured(file_bytes: bytes) -> pd.DataFrame:
    """
    Parse un PDF Hennequin et retourne un DataFrame avec colonnes enrichies.
    
    Retourne un DataFrame avec colonnes:
    - keyDate, Vendor, ProductName, Code_Provider, Date, Prix, Categorie
    - Methode_Peche, Qualite, Decoupe, Etat, Conservation, Origine, Infos_Brutes
    """
    # Utiliser la fonction existante mais retourner un DataFrame au lieu de JSON
    result_list = extract_hennequin_data_from_pdf(file_bytes)
    df = pd.DataFrame(result_list)
    
    # Convertir Date en datetime pour BigQuery (sera converti en DATE lors du chargement)
    if 'Date' in df.columns:
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
    
    # Convertir Prix en float
    if 'Prix' in df.columns:
        df['Prix'] = pd.to_numeric(df['Prix'], errors='coerce')
    
    return df


@app.post("/parseHennequinStructured")
async def parse_hennequin_structured_endpoint(
    file: UploadFile = File(...),
    load_to_bq: bool = Query(False, description="Charger les données dans BigQuery"),
    x_api_key: str = Header(default=None)
):
    """
    Endpoint pour parser Hennequin avec structure complète enrichie.
    Retourne un DataFrame avec toutes les colonnes extraites:
    - Methode_Peche, Qualite, Decoupe, Etat, Conservation, Origine, Infos_Brutes
    """
    if x_api_key != get_api_key():
        raise HTTPException(status_code=403, detail="Invalid API Key")
    
    try:
        file_bytes = await file.read()
        df = parse_hennequin_structured(file_bytes)
        
        # Optionnel: charger dans BigQuery
        if load_to_bq:
            rows_loaded = load_hennequin_structured_to_bigquery(df)
            return {
                "status": "success",
                "rows_parsed": len(df),
                "rows_loaded_to_bq": rows_loaded,
                "sample": sanitize_for_json(df.head(10).to_dict(orient="records"))
            }
        
        return {
            "status": "success",
            "rows_parsed": len(df),
            "data": sanitize_for_json(df.to_dict(orient="records"))
        }
    
    except Exception as e:
        logger.error(f"Erreur parsing Hennequin structured: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/parseHennequinPDF")
async def parse_hennequin_pdf(
    file: UploadFile = File(...),
    background_tasks: BackgroundTasks = BackgroundTasks(),
    x_api_key: str = Header(default=None)
):
    """Nouveau endpoint avec BackgroundTasks et ELT pipeline."""
    if x_api_key != get_api_key():
        raise HTTPException(status_code=403, detail="Invalid API Key")
    
    file_bytes = await file.read()
    return hennequin_service.handle_import(file.filename, file_bytes, background_tasks)


# ============================================================
# Endpoint de status check pour n8n
# ============================================================
@app.get("/jobs/{job_id}")
async def get_job_status(job_id: str):
    """Endpoint pour n8n polling du statut d'un job."""
    from services.bigquery import get_job_status
    
    job = get_job_status(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    
    # Formater la réponse selon le contrat JSON
    return {
        "job_id": job.get("job_id"),
        "status": job.get("status"),
        "vendor": job.get("vendor"),
        "filename": job.get("filename"),
        "gcs_url": job.get("gcs_url"),
        "created_at": job.get("created_at"),
        "completed_at": job.get("completed_at"),
        "duration_seconds": job.get("duration_seconds"),
        "metrics": {
            "rows_extracted": job.get("rows_extracted"),
            "rows_loaded_staging": job.get("rows_loaded_staging"),
            "rows_inserted_prod": job.get("rows_inserted_prod"),
            "rows_updated_prod": job.get("rows_updated_prod"),
            "rows_unknown_products": job.get("rows_unknown_products")
        },
        "error_message": job.get("error_message")
    }


