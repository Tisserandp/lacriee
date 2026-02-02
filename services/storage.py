"""
Service d'archivage GCS pour les fichiers uploadés.
"""
import logging
from datetime import datetime, timedelta
from google.cloud import storage
from google.oauth2 import service_account
import google.auth
import google.auth.transport.requests
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


def download_file(gcs_url: str) -> bytes:
    """
    Télécharge un fichier depuis GCS.

    Args:
        gcs_url: URL complète gs://bucket/path/to/file

    Returns:
        Contenu du fichier en bytes

    Raises:
        Exception: Si le fichier n'existe pas ou erreur de téléchargement
    """
    client = get_gcs_client()

    # Parse gs://bucket/path -> bucket, path
    if not gcs_url.startswith("gs://"):
        raise ValueError(f"URL GCS invalide: {gcs_url}")

    parts = gcs_url.replace("gs://", "").split("/", 1)
    if len(parts) != 2:
        raise ValueError(f"URL GCS invalide: {gcs_url}")

    bucket_name = parts[0]
    blob_path = parts[1]

    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)

    if not blob.exists():
        raise FileNotFoundError(f"Fichier non trouvé dans GCS: {gcs_url}")

    file_bytes = blob.download_as_bytes()
    logger.info(f"Fichier téléchargé: {gcs_url} ({len(file_bytes)} bytes)")

    return file_bytes


def generate_signed_url(gcs_url: str, expiration_minutes: int = 60) -> str:
    """
    Génère une URL signée pour accès temporaire à un fichier GCS.

    Args:
        gcs_url: URL complète gs://bucket/path/to/file
        expiration_minutes: Durée de validité en minutes (défaut: 60)

    Returns:
        URL signée accessible publiquement

    Raises:
        ValueError: Si l'URL GCS est invalide
        FileNotFoundError: Si le fichier n'existe pas
    """
    if not gcs_url.startswith("gs://"):
        raise ValueError(f"URL GCS invalide: {gcs_url}")

    parts = gcs_url.replace("gs://", "").split("/", 1)
    if len(parts) != 2:
        raise ValueError(f"URL GCS invalide: {gcs_url}")

    bucket_name = parts[0]
    blob_path = parts[1]

    # Vérifier si on a un fichier de clé de service account
    sa_key_file = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

    if sa_key_file and os.path.exists(sa_key_file):
        # Avec un fichier de clé SA, on peut signer directement
        credentials = service_account.Credentials.from_service_account_file(sa_key_file)
        client = storage.Client(credentials=credentials)
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_path)

        if not blob.exists():
            raise FileNotFoundError(f"Fichier non trouvé dans GCS: {gcs_url}")

        signed_url = blob.generate_signed_url(
            version="v4",
            expiration=timedelta(minutes=expiration_minutes),
            method="GET",
        )
    else:
        # Cloud Run: utiliser IAM signing avec les credentials par défaut
        credentials, project = google.auth.default()

        if hasattr(credentials, 'refresh'):
            request = google.auth.transport.requests.Request()
            credentials.refresh(request)

        client = storage.Client(credentials=credentials)
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_path)

        if not blob.exists():
            raise FileNotFoundError(f"Fichier non trouvé dans GCS: {gcs_url}")

        signed_url = blob.generate_signed_url(
            version="v4",
            expiration=timedelta(minutes=expiration_minutes),
            method="GET",
            service_account_email=credentials.service_account_email,
            access_token=credentials.token,
        )

    logger.info(f"URL signée générée pour {gcs_url} (expire dans {expiration_minutes} min)")
    return signed_url

