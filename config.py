"""
Configuration et utilitaires pour credentials GCP.
"""
import os
import json
from google.cloud import secretmanager
from google.oauth2 import service_account


def get_secret(secret_id: str) -> str:
    """
    Récupère un secret depuis Secret Manager.
    """
    project_id = os.environ.get("GCP_PROJECT_ID", "lacriee")
    
    # Si c'est un project number (numérique), utiliser le project ID depuis les credentials
    if project_id.isdigit() or not project_id:
        try:
            # Utiliser les credentials pour déterminer le vrai project ID
            from google.auth import default
            credentials, project = default()
            if project:
                project_id = project
            else:
                # Fallback: utiliser "lacriee"
                project_id = "lacriee"
        except Exception:
            # Fallback: utiliser "lacriee" si on ne peut pas déterminer
            project_id = "lacriee"
    
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")


def get_api_key() -> str:
    """
    Récupère la clé API depuis Secret Manager.
    """
    return get_secret("PDF_PARSER_API_KEY")

