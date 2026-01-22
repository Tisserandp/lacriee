"""
Service d'archivage GCS pour les fichiers uploadés.
"""
import logging
from datetime import datetime
from google.cloud import storage
from google.oauth2 import service_account
import json
import os

logger = logging.getLogger(__name__)


def get_gcs_client():
    """
    Retourne un client GCS basé sur les credentials par défaut (Cloud Run, local, etc).
    """
    from google.auth import default

    scopes = ["https://www.googleapis.com/auth/cloud-platform"]
    project_id = os.environ.get("GCP_PROJECT_ID", "lacriee")

    try:
        # Utiliser les credentials par défaut (fonctionne sur Cloud Run, local avec gcloud auth, Docker avec GOOGLE_APPLICATION_CREDENTIALS)
        credentials, project = default(scopes=scopes)
        project_id = project or project_id
        return storage.Client(credentials=credentials, project=project_id)
    except Exception as e:
        logger.error(f"Erreur création client GCS: {e}")
        raise


def archive_file(vendor: str, filename: str, file_bytes: bytes) -> str:
    """
    Archive un fichier dans GCS et retourne l'URL GCS.
    
    Args:
        vendor: Identifiant fournisseur (laurent_daniel, vvqm, demarne, hennequin)
        filename: Nom du fichier original
        file_bytes: Contenu du fichier en bytes
    
    Returns:
        URL GCS du fichier archivé (gs://bucket/path/to/file)
    
    Structure:
        gs://lacriee-archives/{vendor}/{YYYY-MM-DD}/{job_id}_{filename}
    """
    client = get_gcs_client()
    bucket_name = "lacriee-archives"
    
    # Créer le bucket s'il n'existe pas
    try:
        bucket = client.bucket(bucket_name)
        if not bucket.exists():
            bucket = client.create_bucket(bucket_name, location="US")
            logger.info(f"Bucket {bucket_name} créé")
    except Exception as e:
        logger.warning(f"Bucket {bucket_name} existe déjà ou erreur: {e}")
        bucket = client.bucket(bucket_name)
    
    # Structure du chemin: {vendor}/{YYYY-MM-DD}/{filename}
    today = datetime.now().strftime("%Y-%m-%d")
    blob_path = f"{vendor}/{today}/{filename}"
    
    # Upload du fichier
    blob = bucket.blob(blob_path)
    blob.upload_from_string(file_bytes, content_type="application/octet-stream")
    
    gcs_url = f"gs://{bucket_name}/{blob_path}"
    logger.info(f"Fichier archivé: {gcs_url}")
    
    return gcs_url

