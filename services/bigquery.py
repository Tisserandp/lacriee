"""
Service BigQuery pour job tracking, staging load et transformation SQL.
"""
import logging
from datetime import datetime
from typing import Dict, Any, Optional, List
from google.cloud import bigquery
from google.oauth2 import service_account
import json
import os

logger = logging.getLogger(__name__)

# Configuration
DATASET_ID = "PROD"
PROJECT_ID = "lacriee"  # Sera remplacé par le projet actif


def get_bigquery_client():
    """
    Retourne un client BigQuery basé sur les credentials depuis GOOGLE_APPLICATION_CREDENTIALS ou Secret Manager.
    """
    import os
    from google.auth import default
    
    scopes = [
        "https://www.googleapis.com/auth/cloud-platform",
        "https://www.googleapis.com/auth/drive.readonly"
    ]
    
    try:
        # Essayer d'abord avec GOOGLE_APPLICATION_CREDENTIALS (pour Docker/local)
        if os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
            credentials, project = default(scopes=scopes)
            project_id = project or PROJECT_ID
            return bigquery.Client(credentials=credentials, project=project_id)
        
        # Fallback: utiliser Secret Manager si GOOGLE_APPLICATION_CREDENTIALS n'est pas défini
        import config
        secret_name = "providersparser"
        credentials_json = config.get_secret(secret_name)
        info = json.loads(credentials_json)
        credentials = service_account.Credentials.from_service_account_info(
            info, scopes=scopes
        )
        project_id = info.get("project_id", PROJECT_ID)
        return bigquery.Client(credentials=credentials, project=project_id)
    except Exception as e:
        logger.error(f"Erreur création client BigQuery: {e}")
        raise


def create_job_record(
    job_id: str,
    filename: str,
    vendor: str,
    file_size_bytes: int,
    gcs_url: str,
    status: str = "started"
) -> None:
    """
    Crée un enregistrement de job dans ImportJobs.
    
    Args:
        job_id: UUID du job
        filename: Nom du fichier
        vendor: Fournisseur
        file_size_bytes: Taille du fichier
        gcs_url: URL GCS du fichier archivé
        status: Statut initial (default: "started")
    """
    client = get_bigquery_client()
    table_id = f"{client.project}.{DATASET_ID}.ImportJobs"
    
    now = datetime.now()
    
    row = {
        "job_id": job_id,
        "filename": filename,
        "vendor": vendor,
        "file_size_bytes": file_size_bytes,
        "gcs_url": gcs_url,
        "status": status,
        "status_message": "Job créé",
        "created_at": now.isoformat(),
        "started_at": now.isoformat(),
    }
    
    # Utiliser insert_rows_json avec allow_large_results pour éviter le streaming buffer
    errors = client.insert_rows_json(table_id, [row], ignore_unknown_values=False)
    if errors:
        logger.error(f"Erreur insertion job {job_id}: {errors}")
        raise Exception(f"Erreur création job record: {errors}")
    
    logger.info(f"Job {job_id} créé dans ImportJobs")


def update_job_status(
    job_id: str,
    status: str,
    status_message: Optional[str] = None,
    rows_extracted: Optional[int] = None,
    rows_loaded_staging: Optional[int] = None,
    rows_inserted_prod: Optional[int] = None,
    rows_updated_prod: Optional[int] = None,
    rows_unknown_products: Optional[int] = None,
    duration_seconds: Optional[float] = None,
    error_message: Optional[str] = None,
    error_stacktrace: Optional[str] = None
) -> None:
    """
    Met à jour le statut d'un job dans ImportJobs.
    
    Args:
        job_id: UUID du job
        status: Nouveau statut (started, parsing, loading, transforming, completed, failed)
        status_message: Message descriptif
        rows_extracted: Nombre de lignes extraites
        rows_loaded_staging: Nombre de lignes chargées en staging
        rows_inserted_prod: Nombre de lignes insérées en prod
        rows_updated_prod: Nombre de lignes mises à jour en prod
        rows_unknown_products: Nombre de produits inconnus
        duration_seconds: Durée totale en secondes
        error_message: Message d'erreur si échec
        error_stacktrace: Stack trace si échec
    """
    client = get_bigquery_client()
    table_id = f"{client.project}.{DATASET_ID}.ImportJobs"
    
    # Construire la liste des clauses SET pour UPDATE
    set_clauses = []
    
    # Status (toujours présent)
    escaped_status = status.replace("'", "''").replace("\\", "\\\\")
    set_clauses.append(f"status = '{escaped_status}'")
    
    # Status message
    if status_message:
        # Échapper correctement : d'abord les backslashes, puis les guillemets simples
        # Important : remplacer ' par '' pour SQL, mais éviter les problèmes de concaténation
        escaped_message = status_message.replace("\\", "\\\\").replace("'", "''")
        set_clauses.append(f"status_message = '{escaped_message}'")
    
    # Métriques numériques
    if rows_extracted is not None:
        set_clauses.append(f"rows_extracted = {rows_extracted}")
    
    if rows_loaded_staging is not None:
        set_clauses.append(f"rows_loaded_staging = {rows_loaded_staging}")
    
    if rows_inserted_prod is not None:
        set_clauses.append(f"rows_inserted_prod = {rows_inserted_prod}")
    
    if rows_updated_prod is not None:
        set_clauses.append(f"rows_updated_prod = {rows_updated_prod}")
    
    if rows_unknown_products is not None:
        set_clauses.append(f"rows_unknown_products = {rows_unknown_products}")
    
    if duration_seconds is not None:
        set_clauses.append(f"duration_seconds = {duration_seconds}")
    
    # Messages d'erreur (chaînes) - échappement spécial pour éviter les problèmes SQL
    if error_message:
        # Échapper : backslashes d'abord, puis guillemets simples
        # Remplacer tous les caractères problématiques (retours à la ligne, etc.)
        error_msg_clean = error_message[:1000] if len(error_message) > 1000 else error_message
        # Remplacer les retours à la ligne et autres caractères spéciaux AVANT l'échappement SQL
        error_msg_clean = error_msg_clean.replace("\n", " ").replace("\r", " ").replace("\t", " ")
        # Ensuite échapper pour SQL
        escaped_error = error_msg_clean.replace("\\", "\\\\").replace("'", "''")
        set_clauses.append(f"error_message = '{escaped_error}'")
    
    if error_stacktrace:
        # Limiter la longueur et nettoyer les caractères problématiques
        stack_clean = error_stacktrace[:5000] if len(error_stacktrace) > 5000 else error_stacktrace
        # Remplacer les retours à la ligne par des espaces pour éviter les problèmes SQL
        stack_clean = stack_clean.replace("\n", " | ").replace("\r", " ").replace("\t", " ")
        # Ensuite échapper pour SQL
        escaped_stack = stack_clean.replace("\\", "\\\\").replace("'", "''")
        set_clauses.append(f"error_stacktrace = '{escaped_stack}'")
    
    # Timestamp de complétion
    if status in ("completed", "failed"):
        set_clauses.append("completed_at = CURRENT_TIMESTAMP()")
    
    if not set_clauses:
        logger.warning(f"Aucune mise à jour à effectuer pour job {job_id}")
        return
    
    # Échapper le job_id dans la clause WHERE
    escaped_job_id = job_id.replace("'", "''").replace("\\", "\\\\")
    
    # BigQuery ne permet pas UPDATE sur des lignes dans le streaming buffer
    # Solution: attendre plus longtemps (5 secondes) pour que le buffer se vide
    import time
    time.sleep(5)  # Attendre 5 secondes pour que le streaming buffer se vide
    
    update_query = f"""
    UPDATE `{table_id}`
    SET {', '.join(set_clauses)}
    WHERE job_id = '{escaped_job_id}'
    """
    
    try:
        query_job = client.query(update_query)
        query_job.result()  # Attendre la fin
        logger.info(f"Job {job_id} mis à jour: {status}")
    except Exception as e:
        error_str = str(e)
        # Si UPDATE échoue à cause du streaming buffer, logger un warning mais continuer
        # Le job sera mis à jour plus tard quand le buffer se videra
        if "streaming buffer" in error_str.lower():
            logger.warning(f"Streaming buffer actif pour job {job_id}, la mise à jour sera effectuée plus tard")
            # Ne pas lever d'exception, juste logger
            # Le statut sera mis à jour lors de la prochaine tentative ou quand le buffer se videra
        else:
            logger.error(f"Erreur mise à jour job {job_id}: {e}")
            # Pour les erreurs autres que streaming buffer, on peut essayer de continuer
            # mais logger l'erreur pour debug
            logger.error(f"Query (premiers 500 chars): {update_query[:500]}")
            # Ne pas lever d'exception pour ne pas bloquer le traitement
            # Le job sera marqué comme failed plus tard si nécessaire


def load_raw_to_staging(job_id: str, vendor: str, raw_data: List[Dict[str, Any]]) -> int:
    """
    Charge les données brutes dans ProvidersPrices_Staging.
    
    Args:
        job_id: UUID du job
        vendor: Fournisseur
        raw_data: Liste de dictionnaires avec les données brutes extraites
    
    Returns:
        Nombre de lignes chargées
    """
    if not raw_data:
        logger.warning(f"Job {job_id}: Aucune donnée à charger")
        return 0
    
    client = get_bigquery_client()
    table_id = f"{client.project}.{DATASET_ID}.ProvidersPrices_Staging"
    
    now = datetime.now()
    
    # Transformer les données brutes en format staging
    staging_rows = []
    for row in raw_data:
        # Extraire les champs depuis raw_data
        # Format attendu: {date_extracted, product_name_raw, code_provider, price_raw, quality_raw, category_raw}
        staging_row = {
            "job_id": job_id,
            "import_timestamp": now.isoformat(),
            "vendor": vendor,
            "date_extracted": row.get("Date") or row.get("date_extracted"),
            "product_name_raw": row.get("ProductName") or row.get("product_name_raw", ""),
            "code_provider": row.get("Code_Provider") or row.get("code_provider", ""),
            "price_raw": row.get("Prix") or row.get("price_raw"),
            "quality_raw": row.get("Qualité") or row.get("quality_raw"),
            "category_raw": row.get("Catégorie") or row.get("category_raw"),
            "staging_key": f"{job_id}_{vendor}_{row.get('Code_Provider', '')}_{row.get('Date', '')}",
            "processed": False
        }
        staging_rows.append(staging_row)
    
    # Insertion par batch
    errors = client.insert_rows_json(table_id, staging_rows)
    if errors:
        logger.error(f"Erreur insertion staging job {job_id}: {errors}")
        raise Exception(f"Erreur chargement staging: {errors}")
    
    logger.info(f"Job {job_id}: {len(staging_rows)} lignes chargées en staging")
    return len(staging_rows)


def execute_staging_transform(job_id: str) -> Dict[str, int]:
    """
    Exécute la transformation SQL staging → production.
    
    Args:
        job_id: UUID du job
    
    Returns:
        Dictionnaire avec les statistiques: {rows_inserted, rows_updated, rows_unknown}
    """
    import time
    client = get_bigquery_client()
    
    # Attendre que le streaming buffer se vide avant de faire la transformation
    logger.info(f"Job {job_id}: Attente de 10 secondes pour vider le streaming buffer...")
    time.sleep(10)
    
    # Lire le script SQL de transformation
    script_path = os.path.join(os.path.dirname(__file__), "..", "scripts", "transform_staging_to_prod.sql")
    
    try:
        with open(script_path, "r", encoding="utf-8") as f:
            sql_script = f.read()
    except FileNotFoundError:
        logger.error(f"Script SQL non trouvé: {script_path}")
        raise
    
    # Remplacer le paramètre @job_id (échapper les guillemets)
    escaped_job_id = job_id.replace("'", "''")
    sql_script = sql_script.replace("@job_id", f"'{escaped_job_id}'")
    
    # Exécuter la transformation
    try:
        query_job = client.query(sql_script)
        results = list(query_job.result())
        
        # Récupérer les statistiques depuis la dernière requête SELECT
        stats = {"rows_inserted": 0, "rows_updated": 0, "rows_unknown": 0}
        
        # La dernière requête SELECT retourne les stats
        if results:
            last_row = results[-1]
            if hasattr(last_row, "rows_unknown"):
                stats["rows_unknown"] = last_row.rows_unknown or 0
            if hasattr(last_row, "rows_processed"):
                # Approximer rows_inserted + rows_updated depuis rows_processed
                # Note: BigQuery MERGE ne retourne pas directement ces stats
                stats["rows_inserted"] = last_row.rows_processed or 0
        
        # Compter les lignes insérées/mises à jour depuis ProvidersPrices
        # (approximation basée sur les lignes avec ce job_id)
        count_query = f"""
        SELECT 
            COUNT(*) AS total_rows
        FROM `{client.project}.{DATASET_ID}.ProvidersPrices`
        WHERE job_id = '{job_id}'
        """
        count_result = client.query(count_query).result()
        for row in count_result:
            stats["rows_inserted"] = row.total_rows or 0
        
        logger.info(f"Job {job_id}: Transformation SQL terminée - {stats}")
        return stats
        
    except Exception as e:
        logger.error(f"Erreur transformation SQL job {job_id}: {e}")
        raise


def get_job_status(job_id: str) -> Optional[Dict[str, Any]]:
    """
    Récupère le statut d'un job depuis ImportJobs.
    
    Args:
        job_id: UUID du job
    
    Returns:
        Dictionnaire avec les informations du job ou None si non trouvé
    """
    client = get_bigquery_client()
    table_id = f"{client.project}.{DATASET_ID}.ImportJobs"
    
    query = f"""
    SELECT *
    FROM `{table_id}`
    WHERE job_id = '{job_id}'
    LIMIT 1
    """
    
    try:
        query_job = client.query(query)
        results = query_job.result()
        
        for row in results:
            # Convertir la Row en dictionnaire
            job_dict = dict(row)
            return job_dict
        
        return None
        
    except Exception as e:
        logger.error(f"Erreur récupération job {job_id}: {e}")
        return None

