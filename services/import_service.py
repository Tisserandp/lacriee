"""
Service d'import unifié avec archivage, tracking, et async processing automatiques.
"""
import uuid
import logging
from typing import Callable, Dict, Any, Optional
from datetime import datetime
from fastapi import BackgroundTasks

from services.storage import archive_file
from services.bigquery import (
    create_job_record,
    update_job_status,
    load_raw_to_staging,
    execute_staging_transform
)

logger = logging.getLogger(__name__)


class ImportService:
    """
    Orchestre le pipeline complet:
    1. Archivage GCS
    2. Tracking job
    3. Parsing
    4. Chargement staging
    5. Transformation SQL
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
        - Parse
        - Load staging
        - Transform SQL
        - Update job status
        """
        start_time = datetime.now()

        try:
            # 1. PARSING
            update_job_status(job_id, "parsing", "Extracting data from file")
            parser_kwargs = parser_kwargs or {}
            raw_data = self.parser_func(file_bytes, **parser_kwargs)
            rows_extracted = len(raw_data)
            logger.info(f"[{job_id}] Parsed {rows_extracted} rows")

            # 2. LOAD STAGING
            update_job_status(job_id, "loading", f"Loading {rows_extracted} rows to staging")
            rows_loaded = load_raw_to_staging(job_id, self.vendor, raw_data)
            logger.info(f"[{job_id}] Loaded {rows_loaded} rows to staging")

            # 3. TRANSFORM SQL
            update_job_status(job_id, "transforming", "Running SQL transformations")
            transform_result = execute_staging_transform(job_id)

            rows_inserted = transform_result.get("rows_inserted", 0)
            rows_updated = transform_result.get("rows_updated", 0)
            rows_unknown = transform_result.get("rows_unknown", 0)

            duration = (datetime.now() - start_time).total_seconds()

            # 4. COMPLETE
            update_job_status(
                job_id, "completed", "Import completed successfully",
                rows_extracted=rows_extracted,
                rows_loaded_staging=rows_loaded,
                rows_inserted_prod=rows_inserted,
                rows_updated_prod=rows_updated,
                rows_unknown_products=rows_unknown,
                duration_seconds=duration
            )

            logger.info(
                f"[{job_id}] Completed: {rows_extracted} extracted, "
                f"{rows_inserted} inserted, {rows_updated} updated, {rows_unknown} unknown"
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

