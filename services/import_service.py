"""
Service d'import unifié avec archivage, tracking, et async processing automatiques.

Contient aussi les fonctions de chargement spécifiques à chaque provider vers BigQuery.
"""
import uuid
import logging
import pandas as pd
from typing import Callable, Dict, Any, Optional, List
from datetime import datetime
from fastapi import BackgroundTasks
from google.cloud import bigquery
from google.oauth2 import service_account
from pathlib import Path
import json

from services.storage import archive_file
from services.bigquery import (
    create_job_record,
    update_job_status,
    load_to_all_prices
)
from models.schemas import ProductItem

logger = logging.getLogger(__name__)


class ImportService:
    """
    Orchestre le pipeline complet:
    1. Archivage GCS
    2. Tracking job
    3. Parsing (Harmonisé)
    4. Chargement AllPrices (avec déduplication)
    """

    def __init__(self, vendor: str, parser_func: Callable[[bytes], list[dict]]):
        """
        Args:
            vendor: Identifiant fournisseur (laurent_daniel, vvqm, demarne, hennequin)
            parser_func: Fonction de parsing (prend bytes, retourne list[dict])
        """
        self.vendor = vendor
        self.parser_func = parser_func

    def process_sync(self, filename: str, file_bytes: bytes, file_size: int) -> Dict[str, Any]:
        """
        Partie SYNCHRONE (< 1 seconde):
        - Archive fichier GCS
        - Crée job record
        - Retourne job info
        """
        job_id = str(uuid.uuid4())

        try:
            # Archive GCS
            gcs_url = archive_file(self.vendor, filename, file_bytes)
            logger.info(f"[{job_id}] Archived: {gcs_url}")

            # Create job record
            create_job_record(
                job_id=job_id,
                filename=filename,
                vendor=self.vendor,
                file_size_bytes=file_size,
                gcs_url=gcs_url,
                status="started"
            )

            return {
                "job_id": job_id,
                "status": "processing",
                "message": "File received and queued for processing",
                "vendor": self.vendor,
                "filename": filename,
                "gcs_url": gcs_url,
                "check_status_url": f"/jobs/{job_id}"
            }

        except Exception as e:
            logger.exception(f"[{job_id}] Sync error")
            return {"job_id": job_id, "status": "failed", "error": str(e)}

    async def process_async(
        self,
        job_id: str,
        file_bytes: bytes,
        parser_kwargs: Optional[Dict[str, Any]] = None
    ):
        """
        Partie ASYNCHRONE (background):
        - Parse (harmonize=True)
        - Load AllPrices (Merge)
        - Update job status
        """
        start_time = datetime.now()

        try:
            # 1. PARSING
            update_job_status(job_id, "parsing", "Extracting data from file")
            
            # Force harmonization
            parser_kwargs = parser_kwargs or {}
            parser_kwargs["harmonize"] = True
            
            raw_data = self.parser_func(file_bytes, **parser_kwargs)
            rows_extracted = len(raw_data)
            logger.info(f"[{job_id}] Parsed {rows_extracted} rows (Harmonized)")

            # 2. LOAD ALLPRICES
            update_job_status(job_id, "loading", f"Loading {rows_extracted} rows to AllPrices")
            
            load_result = load_to_all_prices(job_id, self.vendor, raw_data)
            
            rows_inserted = load_result.get("rows_inserted", 0)
            rows_updated = load_result.get("rows_updated", 0)
            
            # Note: avec MERGE, on n'a pas toujours le détail exact insert vs update
            # rows_inserted ici contient le total affecté
            
            duration = (datetime.now() - start_time).total_seconds()

            # 3. COMPLETE
            update_job_status(
                job_id, "completed", "Import completed successfully",
                rows_extracted=rows_extracted,
                rows_inserted_prod=rows_inserted,
                rows_updated_prod=rows_updated,
                duration_seconds=duration
            )

            logger.info(
                f"[{job_id}] Completed: {rows_extracted} extracted, "
                f"{rows_inserted} affected in AllPrices"
            )

        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            logger.exception(f"[{job_id}] Async error")

            import traceback
            update_job_status(
                job_id, "failed", str(e),
                error_message=str(e),
                error_stacktrace=traceback.format_exc(),
                duration_seconds=duration
            )

    def handle_import(
        self,
        filename: str,
        file_bytes: bytes,
        background_tasks: BackgroundTasks,
        parser_kwargs: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Handler complet:
        - Sync: Archive + job record + retour immédiat
        - Async: Parse + load + transform (background)
        """
        file_size = len(file_bytes)

        # Sync (rapide)
        response = self.process_sync(filename, file_bytes, file_size)

        # Queue async
        if response["status"] == "processing":
            background_tasks.add_task(
                self.process_async,
                response["job_id"],
                file_bytes,
                parser_kwargs
            )

        return response


# =============================================================================
# BIGQUERY LOADING FUNCTIONS (PROVIDER-SPECIFIC)
# =============================================================================

def get_lacriee_bigquery_client() -> bigquery.Client:
    """
    Get BigQuery client for the lacriee project.

    Tries local credentials first (config/lacrieeparseur.json), then falls back
    to default credentials (for Cloud Run).

    Returns:
        bigquery.Client: Configured BigQuery client
    """
    # Try to load local credentials
    local_creds_path = Path(__file__).parent.parent / "config" / "lacrieeparseur.json"

    if local_creds_path.exists():
        try:
            with open(local_creds_path, 'r') as f:
                creds_info = json.load(f)
            credentials = service_account.Credentials.from_service_account_info(creds_info)
            return bigquery.Client(credentials=credentials, project=creds_info['project_id'])
        except Exception as e:
            logger.warning(f"Failed to load local credentials: {e}. Using default credentials.")

    # Fallback: use default credentials (for Cloud Run)
    return bigquery.Client(project="lacriee")


def load_to_provider_prices(
    data: List[ProductItem],
    table_id: str = "beo-erp.ERPTables.ProvidersPrices"
) -> int:
    """
    Load provider prices data to BigQuery using MERGE pattern.

    This is the legacy endpoint that loads price data to the ProvidersPrices table.
    Uses a MERGE query to insert new records or update existing ones based on keyDate.

    Args:
        data: List of ProductItem objects to load
        table_id: Target table ID

    Returns:
        Number of rows affected
    """
    from services.bq_client import get_bigquery_client

    client = get_bigquery_client()
    dataset_id, table_name = table_id.split(".")[1:]
    temp_table_id = f"{client.project}.{dataset_id}._temp_upload"

    # Convert to DataFrame
    df = pd.DataFrame([row.dict() for row in data])
    df["Date"] = pd.to_datetime(df["Date"], format="%Y-%m-%d", errors="coerce").dt.date

    # Remove duplicates
    df = df.drop_duplicates(subset=["keyDate"], keep="last")

    # Temporary upload
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    job = client.load_table_from_dataframe(df, temp_table_id, job_config=job_config)
    job.result()

    # MERGE on keyDate
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
    logger.info(f"Loaded {len(df)} rows to {table_id}")
    return len(df)


def load_demarne_to_bigquery(
    df: pd.DataFrame,
    table_id: str = "lacriee.PROD.DemarneStructured"
) -> int:
    """
    Load Demarne structured data to BigQuery.

    Args:
        df: DataFrame with Demarne structured data
        table_id: Target table ID

    Returns:
        Number of rows loaded
    """
    client = get_lacriee_bigquery_client()

    # Job configuration with explicit schema
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

    # Load data
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()

    logger.info(f"Loaded {len(df)} rows to {table_id}")
    return len(df)


def load_hennequin_to_bigquery(
    df: pd.DataFrame,
    table_id: str = "lacriee.PROD.HennequinStructured"
) -> int:
    """
    Load Hennequin structured data to BigQuery.

    Args:
        df: DataFrame with Hennequin structured data
        table_id: Target table ID

    Returns:
        Number of rows loaded
    """
    client = get_lacriee_bigquery_client()

    # Ensure Date column is in DATE format for BigQuery
    df = df.copy()
    if 'Date' in df.columns:
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce').dt.date

    # Job configuration with explicit schema
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

    # Load data
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()

    logger.info(f"Loaded {len(df)} rows to {table_id}")
    return len(df)


def load_vvqm_to_bigquery(
    df: pd.DataFrame,
    table_id: str = "lacriee.PROD.VVQMStructured"
) -> int:
    """
    Load VVQM structured data to BigQuery.

    Args:
        df: DataFrame with VVQM structured data
        table_id: Target table ID

    Returns:
        Number of rows loaded
    """
    client = get_lacriee_bigquery_client()

    # Prepare DataFrame for BigQuery
    df_bq = df.copy()
    df_bq["Code"] = df_bq["Code_Provider"]

    # Job configuration with explicit schema
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

    # Select columns in schema order
    cols_to_load = [
        "keyDate", "Code", "Code_Provider", "Espece", "Methode_Peche",
        "Etat", "Decoupe", "Origine", "Section", "Calibre",
        "Prix", "Categorie", "ProductName", "Date", "Vendor"
    ]
    df_bq = df_bq[cols_to_load]

    # Convert Prix to float
    df_bq["Prix"] = pd.to_numeric(df_bq["Prix"], errors="coerce")

    # Load data
    job = client.load_table_from_dataframe(df_bq, table_id, job_config=job_config)
    job.result()

    logger.info(f"Loaded {len(df_bq)} rows to {table_id}")
    return len(df_bq)
