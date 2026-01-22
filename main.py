from fastapi import FastAPI, UploadFile, File, Header, HTTPException, Request, Query, BackgroundTasks
from fastapi.staticfiles import StaticFiles
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware
import fitz  # PyMuPDF
import pandas as pd
import numpy as np
import re
from datetime import date, datetime
import json
from typing import Optional, List
from google.cloud import bigquery, secretmanager
from google.oauth2 import service_account
import logging
import os
import io
import tempfile
from openpyxl import load_workbook
import warnings
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")
from io import BytesIO

# Local imports
import config
from utils.logging import setup_logging, get_logger
from utils.data_cleaning import sanitize_for_json
from models.schemas import ProductItem
from services.import_service import (
    get_lacriee_bigquery_client,
    load_to_provider_prices,
    load_demarne_to_bigquery,
    load_hennequin_to_bigquery,
    load_vvqm_to_bigquery,
)

# Imports des parsers autonomes
from parsers import laurent_daniel, vvqm, demarne, hennequin, audierne

# Setup logging
setup_logging()
logger = get_logger()

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

# NOTE: ProductItem, insert_prices_to_bigquery, sanitize_for_json moved to:
# - ProductItem: models/schemas.py
# - insert_prices_to_bigquery: services/import_service.load_to_provider_prices()
# - sanitize_for_json: utils/data_cleaning.sanitize_for_json()

# NOTE: Toutes les fonctions d'extraction de données ont été migrées vers parsers/
# Utiliser directement les parsers autonomes :
# - from parsers.laurent_daniel import parse as parse_ld
# - from parsers.vvqm import parse as parse_vvqm
# - from parsers.hennequin import parse as parse_hennequin
# - from parsers.demarne import parse as parse_demarne
# - from parsers.audierne import parse as parse_audierne

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
# Page de Test des Parseurs
# ============================================================
@app.get("/test-parser")
async def test_parser_page(request: Request):
    """Page de test interactive pour les parseurs PDF."""
    return templates.TemplateResponse("test_parser.html", {"request": request})


# ============================================================
# Import Services (nouvelle architecture ELT)
# ============================================================
from services.import_service import ImportService

# Initialiser les services d'import avec les parsers autonomes
# Note: Vendors en majuscule pour cohérence avec les données harmonisées
ld_service = ImportService("Laurent Daniel", laurent_daniel.parse)
vvqm_service = ImportService("VVQM", vvqm.parse)
demarne_service = ImportService("Demarne", demarne.parse)
hennequin_service = ImportService("Hennequin", hennequin.parse)
audierne_service = ImportService("Audierne", audierne.parse)


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


@app.post("/api/testLaurentDpdf")
async def test_laurent_d_pdf_api(file: UploadFile = File(...)):
    """
    Endpoint API de TEST pour Laurent Daniel - SANS authentification.
    Retourne les données extraites sans enrichissement pour debug.
    """
    try:
        file_bytes = await file.read()
        data = laurent_daniel.parse(file_bytes, harmonize=False)

        # Convertir en liste de dicts si c'est un DataFrame
        if isinstance(data, pd.DataFrame):
            data = data.to_dict(orient="records")

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


########################################################################################################################
############################################################ Demarne  ##################################################
########################################################################################################################

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
        file_bytes = await file.read()
        df = demarne.parse(file_bytes, harmonize=False, date_fallback=date)
        df_records = df.to_dict(orient="records") if isinstance(df, pd.DataFrame) else df

        # Optionnel: charger dans BigQuery
        if load_to_bq and isinstance(df, pd.DataFrame):
            rows_loaded = load_demarne_structured_to_bigquery(df)
            return {
                "status": "success",
                "rows_parsed": len(df),
                "rows_loaded_to_bq": rows_loaded,
                "sample": df_records[:10]
            }

        return {
            "status": "success",
            "rows_parsed": len(df_records),
            "data": df_records
        }

    except Exception as e:
        logger.error(f"Erreur parsing Demarne structured: {e}")
        raise HTTPException(status_code=500, detail=str(e))


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


########################################################################################################################
############################################################ Hennequin  ##################################################
########################################################################################################################

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
        df = hennequin.parse(file_bytes, harmonize=False)

        # Optionnel: charger dans BigQuery
        if load_to_bq:
            rows_loaded = load_hennequin_structured_to_bigquery(df)
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


########################################################################################################################
#################################################### Audierne ##########################################################
########################################################################################################################
@app.post("/parseAudiernepdf")
async def parse_audierne_pdf(
    file: UploadFile = File(...),
    background_tasks: BackgroundTasks = BackgroundTasks(),
    x_api_key: str = Header(default=None)
):
    """Endpoint pour parser les fichiers PDF Viviers d'Audierne avec ELT pipeline."""
    if x_api_key != get_api_key():
        raise HTTPException(status_code=403, detail="Invalid API Key")

    file_bytes = await file.read()
    return audierne_service.handle_import(file.filename, file_bytes, background_tasks)


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
