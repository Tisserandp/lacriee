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


def extract_LD_data_from_pdf2(file_bytes: bytes):
    
    #path = r"C:\Users\Tisse\OneDrive\Tisserandp\ProvidersParser\CC-84.pdf"
    #doc = fitz.open(stream=open(path, "rb").read(), filetype="pdf")

    
    doc = fitz.open(stream=file_bytes, filetype="pdf")
    raw_lines = []
    
    for page in doc:
        raw_lines += [line.strip() for line in page.get_text().splitlines() if line.strip()]
    
    # 2. Recherche de la date dans les lignes extraites du PDF
    def is_prix(val: str) -> bool:
        return re.match(r"^-?$|^\d+(?:[.,]\d+)?$", val) is not None
    
    date_str = None
    date_pattern = re.compile(r"(\d{1,2})\s+([a-zéû]+)\s+(\d{4})", re.IGNORECASE)
    mois_fr = {
        "janvier": 1, "février": 2, "mars": 3, "avril": 4,
        "mai": 5, "juin": 6, "juillet": 7, "août": 8,
        "septembre": 9, "octobre": 10, "novembre": 11, "décembre": 12
    }
    
    for i, line in enumerate(raw_lines):
        match = date_pattern.search(line)
        if match:
            jour, mois_str, annee = match.groups()
            mois = mois_fr.get(mois_str.lower(), None)
            if mois:
                date_obj = date(int(annee), mois, int(jour))
                date_str = date_obj.isoformat()
    
    # 3. Identifier les produits (triplets : nom + prix + qualité)
    produits = []
    i = 0
    
    while i < len(raw_lines) - 2:
        nom = raw_lines[i].strip()
        prix = raw_lines[i + 1].strip()
        qualite = raw_lines[i + 2].strip()
    
        if nom and is_prix(prix) and qualite:
            produits.append({
                "Produit brut": nom,
                "Prix brut": prix,
                "Qualité": qualite,
                "Index": i
            })
            i += 3
        else:
            i += 1
    
    # 4. Identifier les catégories dynamiques à partir des lignes restantes
    produit_indices = set(p["Index"] for p in produits)
    categorie_candidates = [
        line for idx, line in enumerate(raw_lines)
        if idx not in produit_indices and line.isupper() and len(line) < 40
    ]
    categories = list(set(c.lower().strip() for c in categorie_candidates if c not in ["EURO/Kg", ""]))
    
    # 5. Liste des coquillages connus
    coquillages = [
        "amande", "coque", "bigorneaux gr", "clams", "palourde", "praire",
        "telline", "tourteaux", "vernis", "ormeaux", "venus"
    ]
    
    # 6. Fonction de catégorisation avec les règles métier
    def trouver_categorie_prioritaire(nom_produit):
        nom_min = nom_produit.lower()
    
        if "filet" in nom_min:
            return "FILET"
        if "limande sole" in nom_min:
            return "LIMANDE SOLE"
        if "limande" in nom_min:
            return "LIMANDE"
        if "rgt barbet" in nom_min or "rouget" in nom_min:
            return "ROUGET BARBET"
        if "colin" in nom_min:
            return "MERLU"
        if "portion" in nom_min or "friture" in nom_min:
            return "MERLU"
        if "lgtine" in nom_min:
            return "LANGOUSTINE"
        if "langouste rouge" in nom_min:
            return "HOMARD"
        if "encornet" in nom_min:
            return "ENCORNET"
        if "poulpe" in nom_min:
            return "ENCORNET"
        if "AIGUILLATS" in nom_min:
            return "AIGUILLATS"
        if "pageot" in nom_min or "dorade" in nom_min or "daurade" in nom_min or "pagre" in nom_min:
            return "DORADE / PAGRE"
        if "lieu jaune" in nom_min or "cabillaud" in nom_min or "anon" in nom_min:
            return "LIEU JAUNE / CABILLAUD"
        if any(coq in nom_min for coq in coquillages):
            return "COQUILLAGES"
    
        for cat in categories:
            if cat in nom_min:
                return cat.upper()
    
        return "AUTRES"
    
    # 7. Construction du DataFrame final
    final_data = []
    
    for p in produits:
        prix = None if p["Prix brut"] == "-" else p["Prix brut"].replace(",", ".")
        try:
            prix = float(prix) if prix else None
        except:
            prix = None
        
        final_data.append({
            "Date": date_str,
            "Vendor": "Laurent-Daniel",
            "Catégorie": trouver_categorie_prioritaire(p["Produit brut"]),
            "ProductName": p["Produit brut"],
            "Prix": prix,
            "Qualité": p["Qualité"],

        })
    
    

    df_final = pd.DataFrame(final_data)

    # Liste des produits concernés (en minuscules)
    produits_sans_qualite = [
        "amande",
        "bigorneaux gr",
        "clams",
        "coque",
        "ormeaux",
        "palourde",
        "praire",
        "telline",
        "tourteaux",
        "vernis",
        "venus",
        "katsuobushi",
        "tete/Pate LGTINE",
        "soupe"
    ]
    # Création d'une colonne temporaire en minuscules pour la comparaison
    df_final["Produit_min"] = df_final["ProductName"].str.lower()

    # Mise à jour de la colonne "Qualité"
    df_final.loc[df_final["Produit_min"].isin(produits_sans_qualite), "Qualité"] = ""
    df_final["Vendor"] = "LD"
    df_final["Code_Provider"] = df_final["Vendor"] + "_" + df_final["ProductName"] +  "_" + df_final["Qualité"]
    df_final["Code_Provider"] =  df_final["Code_Provider"].apply(lambda x : x.replace(" ",""))
    df_final["keyDate"] = df_final["Code_Provider"] + "_" + date_str
    #df_final["ProductName"] = df_final["ProductName"] + "_"+ df_final["Qualité"]

    # Suppression de la colonne temporaire si elle n'est plus utile
    df_final.drop(columns=["Produit_min"], inplace=True)

    if not df_final.empty and "Catégorie" in df_final.columns and "ProductName" in df_final.columns:
        df_final = df_final.sort_values(by=["Catégorie", "ProductName"])
    
    df_final = df_final[['Date','Vendor', "keyDate",'Code_Provider','Prix','ProductName',"Catégorie"]]
    df_final.replace([np.inf, -np.inf], None, inplace=True)
    df_final = df_final.where(pd.notnull(df_final), None)

    
        
    try:
        json_ready = sanitize_for_json(df_final)
        logger.info("Conversion JSON OK")
        return json_ready
    except Exception:
        logger.exception("Erreur lors du `sanitize_for_json`")
        raise

import fitz  # PyMuPDF
import pandas as pd
import numpy as np
import re
from datetime import date
from io import BytesIO

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
    COLS = [
        {'name': 'col1', 'x_min': 0,   'x_max': 190},
        {'name': 'col2', 'x_min': 191, 'x_max': 340},
        {'name': 'col3', 'x_min': 341, 'x_max': 600},
    ]
    def is_prix(x0, col_idx):
        if col_idx == 0: return 133 <= x0 <= 150
        if col_idx == 1: return 281 <= x0 <= 299
        if col_idx == 2: return 401 <= x0 <= 460
        return False
    def is_qualite(x0, col_idx):
        if col_idx == 0: return 151 <= x0 <= 190
        if col_idx == 1: return 300 <= x0 <= 340
        if col_idx == 2: return x0 > 461
        return False
    def is_categorie(words, x0s, col_idx):
        if col_idx == 0:
            return min(x0s) >= 55
        if col_idx == 2:
            return min(x0s) >= 360
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
    )
    # Convertir en float seulement les valeurs numériques valides
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
    df_final2 = df_final[['Date','Vendor', "keyDate",'Code_Provider','Prix','ProductName',"Categorie"]]
    
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

    # 🗓️ Ajout des colonnes standard
    df_filtered["Date"] = price_date
    df_filtered["Vendor"] = "Demarne"
    df_filtered["keyDate"] = df_filtered["Code_Provider"] + "_" + df_filtered["Date"]

    # Nettoyage pour JSON
    return sanitize_for_json(df_filtered[["keyDate", "Vendor", "ProductName", "Code_Provider", "Date", "Prix","Categorie"]])



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
############################################################ Old  ############################################################
########################################################################################################################


def parse_vvq_pdf_data(file_bytes: bytes) -> pd.DataFrame:
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
                        # Détection de la date
                        if not date_pdf:
                            m = re.match(r"\b(\d{2})\.(\d{2})\.(\d{4})\b", token)
                            if m:
                                jour, mois, annee = m.groups()
                                date_pdf = f"{annee}-{mois}-{jour}"

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

            rows.append({
                "Produit": produit.strip(),
                "Calibre": calibre.strip(),
                "Prix": val_price.strip()
            })
            used_prices.add((y_price, x_price, val_price))

    df_final = pd.DataFrame(rows).drop_duplicates()
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

    # 📥 Charger la table CodesNames depuis BigQuery
    client = get_bigquery_client()
    codes_names_query = """
        SELECT Code, Name, Categorie
        FROM `lacriee.PROD.CodesNames`
        WHERE Vendor='VVQM'
    """
    codes_names_df = client.query(codes_names_query).result().to_dataframe(create_bqstorage_client=False)
    codes_names_df["Code"] = codes_names_df["Code"].astype(str)

    # 🔗 Faire la jointure
    df_final = df_final.merge(
        codes_names_df,
        left_on="Code_Provider",
        right_on="Code",
        how="left"
    )

    df_final["Categorie"] = df_final["Categorie"].fillna("")

    return df_final[["keyDate", "Vendor", "ProductName", "Code_Provider", "Date", "Prix","Categorie"]]


def test_vvq_parser():
    path = "GEXPORT3.pdf"
    with open(path, "rb") as f:
        file_bytes = f.read()
    df = parse_vvq_pdf_data(file_bytes)
    print(df.head(10))
    return df
    

import fitz
import pandas as pd
import numpy as np
import re
from datetime import datetime
import io

def extract_hennequin_data_from_pdf(file_bytes):
    # 1. Extraction brute des lignes avec coordonnées
    doc = fitz.open(stream=file_bytes, filetype="pdf")
    all_lines = []
    for page_num in range(doc.page_count):
        page = doc[page_num]
        words = page.get_text("words")
        from collections import defaultdict
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

    # 9. Ajout Vendor, Code_Provider, keyDate
    df_final['Vendor'] = 'Hennequin'
    df_final['Code_Provider'] = 'HNQ_' + df_final['ProductName'].str.replace(' ', '_', regex=False).str.lower()
    df_final['keyDate'] = df_final['Code_Provider'] + df_final['Date'].apply(lambda d: f"_{d:%y%m%d}" if pd.notnull(d) else "")

    # 10. Sélection des colonnes finales, nettoyage pour JSON
    df_final = df_final[['Date', 'Vendor', "keyDate", 'Code_Provider', 'Prix', 'ProductName', "Categorie"]]
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


