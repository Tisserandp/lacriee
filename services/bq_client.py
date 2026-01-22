"""
Factory centralisée pour les clients BigQuery.
Évite la duplication du code d'initialisation.
"""
import os
import json
import logging
from pathlib import Path
from typing import Optional
from google.cloud import bigquery
from google.oauth2 import service_account

logger = logging.getLogger(__name__)

# Configuration par défaut
DEFAULT_PROJECT_ID = "lacriee"
DEFAULT_DATASET_ID = "PROD"
DEFAULT_SCOPES = [
    "https://www.googleapis.com/auth/cloud-platform",
    "https://www.googleapis.com/auth/drive.readonly"
]


class BigQueryClientFactory:
    """
    Factory singleton pour créer des clients BigQuery.
    Gère automatiquement les credentials selon l'environnement.
    """

    _clients = {}  # Cache des clients par projet

    @classmethod
    def get_client(
        cls,
        project: str = DEFAULT_PROJECT_ID,
        credentials_file: Optional[str] = None,
        scopes: list = None
    ) -> bigquery.Client:
        """
        Retourne un client BigQuery pour le projet spécifié.
        Utilise un cache pour éviter de recréer les clients.

        Args:
            project: ID du projet GCP (default: lacriee)
            credentials_file: Nom du fichier de credentials (ex: "lacrieeparseur.json")
            scopes: Liste des scopes OAuth

        Returns:
            Client BigQuery configuré
        """
        cache_key = f"{project}_{credentials_file or 'default'}"

        if cache_key in cls._clients:
            return cls._clients[cache_key]

        client = cls._create_client(project, credentials_file, scopes or DEFAULT_SCOPES)
        cls._clients[cache_key] = client
        return client

    @classmethod
    def _create_client(
        cls,
        project: str,
        credentials_file: Optional[str],
        scopes: list
    ) -> bigquery.Client:
        """
        Crée un nouveau client BigQuery.

        Ordre de priorité pour les credentials:
        1. Fichier spécifié dans credentials_file (config/)
        2. Variable d'environnement GOOGLE_APPLICATION_CREDENTIALS
        3. Credentials par défaut (ADC)
        """
        # 1. Fichier de credentials local spécifié
        if credentials_file:
            local_path = Path(__file__).parent.parent / "config" / credentials_file
            if local_path.exists():
                logger.info(f"Utilisation des credentials: {local_path}")
                with open(local_path, 'r') as f:
                    creds_info = json.load(f)
                credentials = service_account.Credentials.from_service_account_info(
                    creds_info, scopes=scopes
                )
                return bigquery.Client(
                    credentials=credentials,
                    project=creds_info.get('project_id', project)
                )

        # 2. Variable d'environnement GOOGLE_APPLICATION_CREDENTIALS
        if os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
            creds_path = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
            logger.info(f"Utilisation de GOOGLE_APPLICATION_CREDENTIALS: {creds_path}")
            if os.path.exists(creds_path):
                with open(creds_path, 'r') as f:
                    creds_info = json.load(f)
                credentials = service_account.Credentials.from_service_account_info(
                    creds_info, scopes=scopes
                )
                return bigquery.Client(
                    credentials=credentials,
                    project=creds_info.get('project_id', project)
                )

        # 3. Credentials par défaut (ADC - pour Cloud Run)
        logger.info(f"Utilisation des credentials par défaut pour projet: {project}")
        from google.auth import default as get_default_credentials
        credentials, detected_project = get_default_credentials(scopes=scopes)
        return bigquery.Client(
            credentials=credentials,
            project=detected_project or project
        )

    @classmethod
    def clear_cache(cls):
        """Vide le cache des clients (utile pour les tests)."""
        cls._clients.clear()


# Raccourcis pour les cas d'usage courants
def get_lacriee_client() -> bigquery.Client:
    """Client pour le projet lacriee (avec lacrieeparseur.json en local)."""
    return BigQueryClientFactory.get_client(
        project="lacriee",
        credentials_file="lacrieeparseur.json"
    )


def get_erp_client() -> bigquery.Client:
    """Client pour le projet beo-erp (avec providersparser.json en local)."""
    return BigQueryClientFactory.get_client(
        project="beo-erp",
        credentials_file="providersparser.json"
    )


def get_default_client() -> bigquery.Client:
    """Client avec les credentials par défaut."""
    return BigQueryClientFactory.get_client()
