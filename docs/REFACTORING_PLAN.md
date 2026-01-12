# Plan de Refactoring : LaCriee Seafood Parser

## Objectif

Transformer le monolithe `main.py` (1087 lignes) en une architecture modulaire maintenable (~10 fichiers), sans changer le comportement fonctionnel.

---

## Etat Actuel

```
lacriee/
├── main.py                 # 1087 lignes - TOUT est ici
├── templates/
│   └── index.html
├── Samples/
│   ├── LaurentD/CC.pdf
│   ├── VVQ/GEXPORT.pdf
│   └── Demarne/Classeur_error.xlsx
├── config/
│   └── providersparser.json
├── Dockerfile
├── docker-compose.yml
└── requirements.txt
```

---

## Etat Cible

```
lacriee/
├── main.py                     # ~100 lignes - Routes FastAPI uniquement
├── config.py                   # ~40 lignes - Configuration et credentials
├── models.py                   # ~30 lignes - Pydantic models
├── services/
│   ├── __init__.py
│   ├── bigquery.py             # ~80 lignes - Operations BigQuery
│   └── storage.py              # ~50 lignes - Archivage GCS
├── parsers/
│   ├── __init__.py             # ~30 lignes - Registre des parsers
│   ├── utils.py                # ~60 lignes - Fonctions utilitaires
│   ├── laurent_daniel.py       # ~180 lignes - Parser Laurent-Daniel
│   ├── vvqm.py                 # ~150 lignes - Parser VVQM
│   ├── demarne.py              # ~100 lignes - Parser Demarne
│   └── hennequin.py            # ~200 lignes - Parser Hennequin
├── templates/
│   └── index.html              # INCHANGE
├── Samples/                    # INCHANGE
├── Dockerfile                  # INCHANGE
├── docker-compose.yml          # INCHANGE
└── requirements.txt            # Ajouter google-cloud-storage
```

---

## Instructions Detaillees par Fichier

### 1. `requirements.txt`

**Action:** Ajouter une dependance

```txt
# Ajouter cette ligne:
google-cloud-storage
```

---

### 2. `config.py` (NOUVEAU)

**Action:** Creer ce fichier

**Contenu a extraire de main.py:**
- Lignes 29-56: Configuration logging
- Lignes 58-82: Fonctions `get_secret()`, `get_api_key()`, `get_credentials_from_secret_json()`, `get_bigquery_client()`
- Variable `GCP_PROJECT_ID`

**Code attendu:**

```python
"""
Configuration et gestion des credentials GCP.
"""
import os
import json
import logging
import re
from google.cloud import secretmanager, bigquery
from google.oauth2 import service_account

# ============================================================
# LOGGING CONFIGURATION
# ============================================================

LOG_DIR = "./logs"
LOG_PATH = os.path.join(LOG_DIR, "pdf_parser.log")

if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

class SafeConsoleFormatter(logging.Formatter):
    def format(self, record):
        msg = super().format(record)
        return re.sub(r'[\ud800-\udfff]', '', msg)

# File handler
file_handler = logging.FileHandler(LOG_PATH, encoding='utf-8')
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(file_handler)

# Console handler
console_handler = logging.StreamHandler()
console_handler.setFormatter(SafeConsoleFormatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(console_handler)

# ============================================================
# GCP CONFIGURATION
# ============================================================

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")

def get_secret(secret_id: str) -> str:
    """Recupere un secret depuis Google Secret Manager."""
    project_id = GCP_PROJECT_ID
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")

def get_api_key() -> str:
    """Recupere la cle API depuis Secret Manager."""
    return get_secret("PDF_PARSER_API_KEY")

def get_credentials_from_secret_json(
    secret_name: str = "providersparser",
    scopes: list = None
) -> service_account.Credentials:
    """Cree des credentials depuis un secret JSON."""
    if scopes is None:
        scopes = [
            "https://www.googleapis.com/auth/cloud-platform",
            "https://www.googleapis.com/auth/drive.readonly"
        ]
    credentials_json = get_secret(secret_name)
    info = json.loads(credentials_json)
    return service_account.Credentials.from_service_account_info(info, scopes=scopes)

def get_bigquery_client(
    secret_name: str = "providersparser",
    scopes: list = None
) -> bigquery.Client:
    """Cree un client BigQuery avec les credentials du secret."""
    if scopes is None:
        scopes = [
            "https://www.googleapis.com/auth/cloud-platform",
            "https://www.googleapis.com/auth/drive.readonly"
        ]
    credentials_json = get_secret(secret_name)
    info = json.loads(credentials_json)
    credentials = service_account.Credentials.from_service_account_info(info, scopes=scopes)
    return bigquery.Client(credentials=credentials, project=info["project_id"])
```

---

### 3. `models.py` (NOUVEAU)

**Action:** Creer ce fichier

**Contenu a extraire de main.py:**
- Lignes 84-91: Classe `ProductItem`

**Code attendu:**

```python
"""
Modeles Pydantic pour les donnees.
"""
from pydantic import BaseModel
from typing import Optional

class ProductItem(BaseModel):
    """Schema d'un produit parse."""
    keyDate: str
    Vendor: str
    ProductName: str
    Code_Provider: str
    Date: str
    Prix: Optional[float] = None
    Categorie: Optional[str] = None
```

---

### 4. `services/__init__.py` (NOUVEAU)

**Action:** Creer ce fichier vide

```python
"""Services module."""
```

---

### 5. `services/bigquery.py` (NOUVEAU)

**Action:** Creer ce fichier

**Contenu a extraire de main.py:**
- Lignes 93-127: Fonction `insert_prices_to_bigquery()`
- Lignes 129-144: Fonction `sanitize_for_json()`

**Code attendu:**

```python
"""
Operations BigQuery: insertion et requetes.
"""
import logging
import pandas as pd
import numpy as np
from typing import List
from google.cloud import bigquery

from config import get_bigquery_client
from models import ProductItem

logger = logging.getLogger(__name__)

def sanitize_for_json(df: pd.DataFrame) -> list[dict]:
    """
    Nettoie un DataFrame pour serialisation JSON.
    Remplace inf, -inf, NaN par None.
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

def insert_prices_to_bigquery(
    data: List[ProductItem],
    table_id: str = "beo-erp.ERPTables.ProvidersPrices"
) -> int:
    """
    Insere ou met a jour les prix dans BigQuery via MERGE.
    Retourne le nombre de lignes affectees.
    """
    client = get_bigquery_client()

    # Convertir en DataFrame
    df = pd.DataFrame([item.dict() for item in data])
    df["Date"] = pd.to_datetime(df["Date"]).dt.date
    df = df.drop_duplicates(subset=["keyDate"], keep="last")

    # Charger dans table temporaire
    dataset = table_id.rsplit(".", 1)[0]
    temp_table_id = f"{dataset}._temp_upload"

    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    job = client.load_table_from_dataframe(df, temp_table_id, job_config=job_config)
    job.result()

    # MERGE vers table de production
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

    query_job = client.query(merge_query)
    query_job.result()

    logger.info(f"MERGE completed: {len(df)} rows processed")
    return len(df)

def query_codes_names(vendor: str) -> pd.DataFrame:
    """
    Recupere la table de mapping CodesNames pour un vendor.
    """
    client = get_bigquery_client()
    query = f"""
        SELECT Code, Name, Categorie
        FROM `beo-erp.ERPTables.CodesNames`
        WHERE Vendor='{vendor}'
    """
    return client.query(query).result().to_dataframe()
```

---

### 6. `services/storage.py` (NOUVEAU)

**Action:** Creer ce fichier

**Code attendu:**

```python
"""
Archivage des fichiers dans Google Cloud Storage.
"""
import logging
import uuid
from datetime import date
from google.cloud import storage

from config import get_credentials_from_secret_json

logger = logging.getLogger(__name__)

BUCKET_NAME = "lacriee-archives"

def get_storage_client() -> storage.Client:
    """Cree un client GCS avec les credentials."""
    credentials = get_credentials_from_secret_json()
    return storage.Client(credentials=credentials, project=credentials.project_id)

def archive_file(vendor: str, filename: str, file_bytes: bytes) -> str:
    """
    Archive un fichier dans GCS.

    Structure: gs://lacriee-archives/{vendor}/{YYYY-MM-DD}/{uuid}_{filename}

    Args:
        vendor: Nom du fournisseur (laurent_daniel, vvqm, demarne, hennequin)
        filename: Nom original du fichier
        file_bytes: Contenu du fichier

    Returns:
        URI GCS du fichier archive (gs://...)
    """
    client = get_storage_client()
    bucket = client.bucket(BUCKET_NAME)

    # Generer un nom unique
    today = date.today().isoformat()
    unique_id = uuid.uuid4().hex[:8]
    blob_name = f"{vendor}/{today}/{unique_id}_{filename}"

    # Upload
    blob = bucket.blob(blob_name)
    blob.upload_from_string(file_bytes)

    gcs_uri = f"gs://{BUCKET_NAME}/{blob_name}"
    logger.info(f"File archived: {gcs_uri}")

    return gcs_uri
```

---

### 7. `parsers/__init__.py` (NOUVEAU)

**Action:** Creer ce fichier

**Code attendu:**

```python
"""
Registre des parsers par fournisseur.
"""
from .laurent_daniel import parse as parse_laurent_daniel
from .vvqm import parse as parse_vvqm
from .demarne import parse as parse_demarne
from .hennequin import parse as parse_hennequin

# Registre: vendor_id -> fonction de parsing
PARSERS = {
    "laurent_daniel": parse_laurent_daniel,
    "vvqm": parse_vvqm,
    "demarne": parse_demarne,
    "hennequin": parse_hennequin,
}

def get_parser(vendor: str):
    """Retourne la fonction de parsing pour un vendor."""
    return PARSERS.get(vendor)

def list_vendors() -> list[str]:
    """Liste tous les vendors supportes."""
    return list(PARSERS.keys())
```

---

### 8. `parsers/utils.py` (NOUVEAU)

**Action:** Creer ce fichier

**Contenu a extraire de main.py:**
- Lignes 145-146: Fonction `is_prix()`
- Autres fonctions utilitaires communes aux parsers

**Code attendu:**

```python
"""
Fonctions utilitaires communes aux parsers.
"""
import re
import pandas as pd
import numpy as np
from typing import Optional

def is_prix(val: str) -> bool:
    """
    Verifie si une valeur est un prix valide.
    Accepte: vide, tiret, entier, decimal (virgule ou point).
    """
    if val is None:
        return False
    return re.match(r"^-?$|^\d+(?:[.,]\d+)?$", str(val).strip()) is not None

def parse_french_date(text: str) -> Optional[str]:
    """
    Parse une date francaise (ex: "15 janvier 2024") en format ISO.
    Retourne None si non trouve.
    """
    mois_fr = {
        "janvier": "01", "février": "02", "mars": "03", "avril": "04",
        "mai": "05", "juin": "06", "juillet": "07", "août": "08",
        "septembre": "09", "octobre": "10", "novembre": "11", "décembre": "12"
    }

    pattern = r"(\d{1,2})\s+(janvier|février|mars|avril|mai|juin|juillet|août|septembre|octobre|novembre|décembre)\s+(\d{4})"
    match = re.search(pattern, text.lower())

    if match:
        jour = match.group(1).zfill(2)
        mois = mois_fr[match.group(2)]
        annee = match.group(3)
        return f"{annee}-{mois}-{jour}"

    return None

def parse_date_ddmmyyyy(text: str, separator: str = "/") -> Optional[str]:
    """
    Parse une date DD/MM/YYYY ou DD.MM.YYYY en format ISO.
    """
    pattern = rf"(\d{{2}}){re.escape(separator)}(\d{{2}}){re.escape(separator)}(\d{{4}})"
    match = re.search(pattern, text)

    if match:
        jour, mois, annee = match.groups()
        return f"{annee}-{mois}-{jour}"

    return None

def sanitize_for_json(df: pd.DataFrame) -> list[dict]:
    """
    Nettoie un DataFrame pour serialisation JSON.
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

def clean_product_name(name: str) -> str:
    """Nettoie un nom de produit."""
    if not name:
        return ""
    return " ".join(name.split()).strip()
```

---

### 9. `parsers/laurent_daniel.py` (NOUVEAU)

**Action:** Creer ce fichier

**Contenu a extraire de main.py:**
- Lignes 149-253: Fonction `trouver_categorie_prioritaire()` et regles de categorisation
- Lignes 334-483: Fonction `extract_LD_data_from_pdf()`
- Lignes 280-326: Logique de nettoyage specifique LD

**IMPORTANT:** Copier la logique EXACTEMENT, ne pas modifier l'algorithme d'extraction.

**Structure attendue:**

```python
"""
Parser pour les PDFs Laurent-Daniel.
"""
import re
import logging
import fitz  # PyMuPDF
import pandas as pd
from typing import List
from datetime import datetime

from models import ProductItem
from .utils import is_prix, parse_french_date, sanitize_for_json

logger = logging.getLogger(__name__)

# ============================================================
# REGLES DE CATEGORISATION LAURENT-DANIEL
# ============================================================

def trouver_categorie_prioritaire(nom_produit: str) -> str:
    """
    Determine la categorie d'un produit selon les regles LD.
    COPIE EXACTE de main.py lignes 217-253.
    """
    nom_min = nom_produit.lower()

    # [COPIER ICI LA LOGIQUE EXACTE DE main.py lignes 217-253]
    # Ne pas modifier cette logique!

    if "filet" in nom_min:
        return "FILET"
    if "limande sole" in nom_min:
        return "LIMANDE SOLE"
    if "limande" in nom_min:
        return "LIMANDE"
    # ... etc (copier TOUTES les regles)

    return "AUTRES"

# Liste des produits sans qualite
PRODUITS_SANS_QUALITE = [
    "amande", "bigorneaux gr", "clams", "coque", "ormeaux", "palourde",
    "praire", "telline", "tourteaux", "vernis", "venus", "katsuobushi",
    "tete/Pate LGTINE", "soupe"
]

# ============================================================
# FONCTION PRINCIPALE DE PARSING
# ============================================================

def parse(file_bytes: bytes) -> List[dict]:
    """
    Parse un PDF Laurent-Daniel et retourne les produits extraits.

    COPIE EXACTE de main.py lignes 334-483 (extract_LD_data_from_pdf).

    Args:
        file_bytes: Contenu binaire du PDF

    Returns:
        Liste de dictionnaires avec les produits
    """
    # [COPIER ICI LA LOGIQUE EXACTE DE extract_LD_data_from_pdf]
    # Ne pas modifier cette logique!

    doc = fitz.open(stream=file_bytes, filetype="pdf")

    # ... (copier tout le code d'extraction)

    # A la fin, retourner les donnees sanitisees
    return sanitize_for_json(df_final)
```

---

### 10. `parsers/vvqm.py` (NOUVEAU)

**Action:** Creer ce fichier

**Contenu a extraire de main.py:**
- Lignes 738-880: Fonction `parse_vvq_pdf_data()`

**IMPORTANT:** Copier la logique EXACTEMENT.

**Structure attendue:**

```python
"""
Parser pour les PDFs VVQM.
"""
import re
import logging
import fitz  # PyMuPDF
import pandas as pd
from typing import List

from models import ProductItem
from services.bigquery import query_codes_names
from .utils import parse_date_ddmmyyyy, sanitize_for_json

logger = logging.getLogger(__name__)

def parse(file_bytes: bytes) -> List[dict]:
    """
    Parse un PDF VVQM et retourne les produits extraits.

    COPIE EXACTE de main.py lignes 738-880 (parse_vvq_pdf_data).

    Args:
        file_bytes: Contenu binaire du PDF

    Returns:
        Liste de dictionnaires avec les produits
    """
    # [COPIER ICI LA LOGIQUE EXACTE DE parse_vvq_pdf_data]
    # Ne pas modifier cette logique!

    doc = fitz.open(stream=file_bytes, filetype="pdf")

    # ... (copier tout le code d'extraction)

    # Jointure avec CodesNames
    codes_names_df = query_codes_names("VVQM")

    # ... (copier la logique de jointure)

    return sanitize_for_json(df_final)
```

---

### 11. `parsers/demarne.py` (NOUVEAU)

**Action:** Creer ce fichier

**Contenu a extraire de main.py:**
- Lignes 583-611: Fonction `extract_date_from_excel_header()`
- Lignes 627-676: Fonction `parse_demarne_excel_data()`

**Structure attendue:**

```python
"""
Parser pour les fichiers Excel Demarne.
"""
import re
import logging
import pandas as pd
from typing import List, Optional
from openpyxl import load_workbook
from datetime import datetime
import tempfile
import os

from models import ProductItem
from services.bigquery import query_codes_names
from .utils import sanitize_for_json

logger = logging.getLogger(__name__)

def extract_date_from_excel_header(path: str, date_fallback: Optional[str] = None) -> str:
    """
    Extrait la date depuis le header Excel.

    COPIE EXACTE de main.py lignes 583-611.
    """
    # [COPIER ICI LA LOGIQUE EXACTE]
    pass

def parse(file_bytes: bytes, date_fallback: Optional[str] = None) -> List[dict]:
    """
    Parse un fichier Excel Demarne.

    COPIE EXACTE de main.py lignes 627-676 (parse_demarne_excel_data).

    Args:
        file_bytes: Contenu binaire du fichier Excel
        date_fallback: Date de fallback si non trouvee dans le fichier

    Returns:
        Liste de dictionnaires avec les produits
    """
    # Sauvegarder temporairement le fichier (openpyxl a besoin d'un path)
    with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as tmp:
        tmp.write(file_bytes)
        tmp_path = tmp.name

    try:
        # [COPIER ICI LA LOGIQUE EXACTE DE parse_demarne_excel_data]

        # Extraire la date
        date_str = extract_date_from_excel_header(tmp_path, date_fallback)

        # Lire Excel
        df = pd.read_excel(tmp_path, header=2)

        # ... (copier la logique)

        # Jointure avec CodesNames
        codes_names_df = query_codes_names("Demarne")

        # ... (copier la logique de jointure)

        return sanitize_for_json(df_final)

    finally:
        os.unlink(tmp_path)
```

---

### 12. `parsers/hennequin.py` (NOUVEAU)

**Action:** Creer ce fichier

**Contenu a extraire de main.py:**
- Lignes 890-1070: Fonction `extract_hennequin_data_from_pdf()`
- Lignes 1042-1049: Mapping des categories

**Structure attendue:**

```python
"""
Parser pour les PDFs Hennequin.
"""
import re
import logging
import fitz  # PyMuPDF
import pandas as pd
from typing import List

from models import ProductItem
from .utils import parse_date_ddmmyyyy, sanitize_for_json

logger = logging.getLogger(__name__)

# Mapping des categories Hennequin
CATEGORY_MAP = {
    'BAR PETIT BATEAU': 'BAR',
    'BAR LIGNE': 'BAR',
    'DORADE ROYALE': 'DORADE',
    'DORADE SAR': 'DORADE'
}

def parse(file_bytes: bytes) -> List[dict]:
    """
    Parse un PDF Hennequin et retourne les produits extraits.

    COPIE EXACTE de main.py lignes 890-1070 (extract_hennequin_data_from_pdf).

    Args:
        file_bytes: Contenu binaire du PDF

    Returns:
        Liste de dictionnaires avec les produits
    """
    # [COPIER ICI LA LOGIQUE EXACTE DE extract_hennequin_data_from_pdf]
    # Ne pas modifier cette logique!

    doc = fitz.open(stream=file_bytes, filetype="pdf")

    # ... (copier tout le code d'extraction)

    # Appliquer le mapping des categories
    df_final['Categorie'] = df_final['Catégorie'].replace(CATEGORY_MAP)

    return sanitize_for_json(df_final)
```

---

### 13. `main.py` (REFACTORING)

**Action:** Remplacer le contenu par une version simplifiee

**Code attendu:**

```python
"""
LaCriee Seafood Parser - API FastAPI
"""
import logging
from typing import Optional
from fastapi import FastAPI, File, UploadFile, Header, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.requests import Request

from config import get_api_key
from models import ProductItem
from services.bigquery import insert_prices_to_bigquery
from services.storage import archive_file
from parsers import get_parser
from parsers.laurent_daniel import parse as parse_laurent_daniel
from parsers.vvqm import parse as parse_vvqm
from parsers.demarne import parse as parse_demarne
from parsers.hennequin import parse as parse_hennequin

logger = logging.getLogger(__name__)

# ============================================================
# APPLICATION FASTAPI
# ============================================================

app = FastAPI(title="LaCriee Seafood Parser", version="2.0.0")

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Static files & templates
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# ============================================================
# VALIDATION API KEY
# ============================================================

def validate_api_key(x_api_key: str) -> bool:
    """Valide la cle API."""
    if not x_api_key:
        return False
    return x_api_key == get_api_key()

# ============================================================
# ENDPOINTS
# ============================================================

@app.get("/")
async def home(request: Request):
    """Page d'accueil."""
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/health")
async def health():
    """Health check."""
    return {"status": "healthy"}

# ------------------------------------------------------------
# LAURENT-DANIEL
# ------------------------------------------------------------

@app.post("/parseLaurentDpdf")
async def parse_laurent_d_pdf(
    file: UploadFile = File(...),
    push_to_bq: bool = False,
    x_api_key: str = Header(default=None)
):
    """Parse un PDF Laurent-Daniel."""
    if not validate_api_key(x_api_key):
        raise HTTPException(status_code=401, detail="Invalid API key")

    file_bytes = await file.read()

    # Archiver le fichier
    try:
        archive_file("laurent_daniel", file.filename, file_bytes)
    except Exception as e:
        logger.warning(f"Failed to archive file: {e}")

    # Parser
    data = parse_laurent_daniel(file_bytes)

    # Push to BigQuery si demande
    if push_to_bq and data:
        items = [ProductItem(**item) for item in data]
        insert_prices_to_bigquery(items)

    return data

@app.get("/parseLaurentDpdf")
async def parse_laurent_d_pdf_local(x_api_key: str = Header(default=None)):
    """Test avec fichier local."""
    if not validate_api_key(x_api_key):
        raise HTTPException(status_code=401, detail="Invalid API key")

    with open("Samples/LaurentD/CC.pdf", "rb") as f:
        file_bytes = f.read()

    return parse_laurent_daniel(file_bytes)

# ------------------------------------------------------------
# VVQM
# ------------------------------------------------------------

@app.post("/parseVVQpdf")
async def parse_vvq_pdf(
    file: UploadFile = File(...),
    push_to_bq: bool = False,
    x_api_key: str = Header(default=None)
):
    """Parse un PDF VVQM."""
    if not validate_api_key(x_api_key):
        raise HTTPException(status_code=401, detail="Invalid API key")

    file_bytes = await file.read()

    # Archiver
    try:
        archive_file("vvqm", file.filename, file_bytes)
    except Exception as e:
        logger.warning(f"Failed to archive file: {e}")

    # Parser
    data = parse_vvqm(file_bytes)

    if push_to_bq and data:
        items = [ProductItem(**item) for item in data]
        insert_prices_to_bigquery(items)

    return data

@app.get("/parseVVQpdf_test")
async def parse_vvq_pdf_test():
    """Test avec fichier local."""
    with open("Samples/VVQ/GEXPORT.pdf", "rb") as f:
        file_bytes = f.read()

    return parse_vvqm(file_bytes)

# ------------------------------------------------------------
# DEMARNE
# ------------------------------------------------------------

@app.post("/parseDemarneXLS")
async def parse_demarne_xls(
    file: UploadFile = File(...),
    push_to_bq: bool = False,
    date: Optional[str] = Query(None),
    x_api_key: str = Header(default=None)
):
    """Parse un fichier Excel Demarne."""
    if not validate_api_key(x_api_key):
        raise HTTPException(status_code=401, detail="Invalid API key")

    file_bytes = await file.read()

    # Archiver
    try:
        archive_file("demarne", file.filename, file_bytes)
    except Exception as e:
        logger.warning(f"Failed to archive file: {e}")

    # Parser
    data = parse_demarne(file_bytes, date_fallback=date)

    if push_to_bq and data:
        items = [ProductItem(**item) for item in data]
        insert_prices_to_bigquery(items)

    return data

@app.get("/parseDemarneXLS")
async def parse_demarne_xls_local(x_api_key: str = Header(default=None)):
    """Test avec fichier local."""
    if not validate_api_key(x_api_key):
        raise HTTPException(status_code=401, detail="Invalid API key")

    with open("Samples/Demarne/Classeur_error.xlsx", "rb") as f:
        file_bytes = f.read()

    return parse_demarne(file_bytes)

# ------------------------------------------------------------
# HENNEQUIN
# ------------------------------------------------------------

@app.post("/parseHennequinPDF")
async def parse_hennequin_pdf(
    file: UploadFile = File(...),
    push_to_bq: bool = False,
    x_api_key: str = Header(default=None)
):
    """Parse un PDF Hennequin."""
    if not validate_api_key(x_api_key):
        raise HTTPException(status_code=401, detail="Invalid API key")

    file_bytes = await file.read()

    # Archiver
    try:
        archive_file("hennequin", file.filename, file_bytes)
    except Exception as e:
        logger.warning(f"Failed to archive file: {e}")

    # Parser
    data = parse_hennequin(file_bytes)

    if push_to_bq and data:
        items = [ProductItem(**item) for item in data]
        insert_prices_to_bigquery(items)

    return data

# ============================================================
# ENDPOINT TESTDATE (conserve de l'original)
# ============================================================

@app.post("/testDate")
async def test_date(file: UploadFile = File(...)):
    """Test d'extraction de date depuis un Excel."""
    from parsers.demarne import extract_date_from_excel_header
    import tempfile
    import os

    file_bytes = await file.read()

    with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as tmp:
        tmp.write(file_bytes)
        tmp_path = tmp.name

    try:
        date_str = extract_date_from_excel_header(tmp_path)
        return {"date": date_str}
    finally:
        os.unlink(tmp_path)
```

---

## Infrastructure GCS

### Creer le bucket

Executer dans Cloud Shell ou avec `gcloud`:

```bash
# Creer le bucket
gcloud storage buckets create gs://lacriee-archives \
    --project=beo-erp \
    --location=europe-west1 \
    --uniform-bucket-level-access

# Optionnel: Configurer lifecycle (supprimer fichiers > 1 an)
cat > lifecycle.json << 'EOF'
{
  "lifecycle": {
    "rule": [
      {
        "action": {"type": "Delete"},
        "condition": {"age": 365}
      }
    ]
  }
}
EOF

gcloud storage buckets update gs://lacriee-archives --lifecycle-file=lifecycle.json
```

### Permissions

Le service account `providersparser@beo-erp.iam.gserviceaccount.com` doit avoir le role `Storage Object Admin` sur le bucket:

```bash
gcloud storage buckets add-iam-policy-binding gs://lacriee-archives \
    --member="serviceAccount:providersparser@beo-erp.iam.gserviceaccount.com" \
    --role="roles/storage.objectAdmin"
```

---

## Tests de Validation

### 1. Test unitaire de chaque parser

```bash
# Activer l'environnement
cd lacriee

# Tester Laurent-Daniel
python -c "
from parsers.laurent_daniel import parse
with open('Samples/LaurentD/CC.pdf', 'rb') as f:
    data = parse(f.read())
print(f'Laurent-Daniel: {len(data)} produits')
print(data[0] if data else 'Aucun produit')
"

# Tester VVQM
python -c "
from parsers.vvqm import parse
with open('Samples/VVQ/GEXPORT.pdf', 'rb') as f:
    data = parse(f.read())
print(f'VVQM: {len(data)} produits')
"

# Tester Demarne
python -c "
from parsers.demarne import parse
with open('Samples/Demarne/Classeur_error.xlsx', 'rb') as f:
    data = parse(f.read())
print(f'Demarne: {len(data)} produits')
"
```

### 2. Test de non-regression

Comparer les sorties avant/apres refactoring:

```bash
# Avant (avec ancien main.py)
curl -X POST "http://localhost:8080/parseLaurentDpdf" \
    -H "x-api-key: YOUR_KEY" \
    -F "file=@Samples/LaurentD/CC.pdf" > output_before.json

# Apres (avec nouveau code)
curl -X POST "http://localhost:8080/parseLaurentDpdf" \
    -H "x-api-key: YOUR_KEY" \
    -F "file=@Samples/LaurentD/CC.pdf" > output_after.json

# Comparer
diff output_before.json output_after.json
```

### 3. Test d'archivage GCS

```bash
# Verifier que le fichier est archive
gcloud storage ls gs://lacriee-archives/laurent_daniel/$(date +%Y-%m-%d)/
```

---

## Checklist de Migration

- [ ] Ajouter `google-cloud-storage` a requirements.txt
- [ ] Creer `config.py`
- [ ] Creer `models.py`
- [ ] Creer `services/__init__.py`
- [ ] Creer `services/bigquery.py`
- [ ] Creer `services/storage.py`
- [ ] Creer `parsers/__init__.py`
- [ ] Creer `parsers/utils.py`
- [ ] Creer `parsers/laurent_daniel.py` (copier logique exacte)
- [ ] Creer `parsers/vvqm.py` (copier logique exacte)
- [ ] Creer `parsers/demarne.py` (copier logique exacte)
- [ ] Creer `parsers/hennequin.py` (copier logique exacte)
- [ ] Refactorer `main.py`
- [ ] Creer bucket GCS `lacriee-archives`
- [ ] Configurer permissions GCS
- [ ] Tester chaque parser unitairement
- [ ] Tester les endpoints API
- [ ] Verifier archivage GCS
- [ ] Comparer sorties avant/apres (non-regression)
- [ ] Deployer sur Cloud Run

---

## Notes Importantes

1. **NE PAS MODIFIER LA LOGIQUE D'EXTRACTION** - Copier le code existant tel quel
2. **Les imports doivent etre ajustes** selon la nouvelle structure
3. **Le bucket GCS doit exister** avant de deployer
4. **Tester localement** avant de deployer sur Cloud Run
