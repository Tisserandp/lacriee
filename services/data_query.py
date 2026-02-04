"""
Service de requete flexible sur AllPrices pour analyse qualite.
"""
import logging
from typing import Dict, Any, Optional, List
from .bigquery import get_bigquery_client, DATASET_ID

logger = logging.getLogger(__name__)


def query_all_prices(
    vendor: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    categorie: Optional[str] = None,
    fields: Optional[List[str]] = None,
    limit: int = 100,
    offset: int = 0
) -> List[Dict[str, Any]]:
    """
    Requete flexible sur AllPrices avec filtres optionnels.

    Args:
        vendor: Filtrer par vendor (Demarne, Audierne, etc.)
        date_from: Date debut (YYYY-MM-DD)
        date_to: Date fin (YYYY-MM-DD)
        categorie: Filtrer par categorie harmonisee
        fields: Liste des champs a retourner (default: tous)
        limit: Nombre max de lignes (default: 100, max: 1000)
        offset: Offset pour pagination

    Returns:
        Liste de dictionnaires avec les produits
    """
    client = get_bigquery_client()
    table_id = f"{client.project}.{DATASET_ID}.AllPrices"

    # Champs par defaut
    default_fields = [
        "key_date", "date", "vendor", "code_provider", "product_name", "prix",
        "categorie", "methode_peche", "qualite", "decoupe", "etat", "origine", "calibre",
        "type_production", "couleur",
        "conservation", "trim", "label", "variante"
    ]

    select_fields = fields if fields else default_fields
    select_clause = ", ".join(select_fields)

    # Construire WHERE
    where_clauses = []

    if vendor:
        escaped_vendor = vendor.replace("'", "''")
        where_clauses.append(f"vendor = '{escaped_vendor}'")

    if date_from:
        escaped_date = date_from.replace("'", "''")
        where_clauses.append(f"date >= '{escaped_date}'")

    if date_to:
        escaped_date = date_to.replace("'", "''")
        where_clauses.append(f"date <= '{escaped_date}'")

    if categorie:
        escaped_cat = categorie.replace("'", "''")
        where_clauses.append(f"categorie = '{escaped_cat}'")

    where_clause = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

    # Limit securise
    limit = min(limit, 1000)

    query = f"""
    SELECT {select_clause}
    FROM `{table_id}`
    {where_clause}
    ORDER BY date DESC, vendor, product_name
    LIMIT {limit}
    OFFSET {offset}
    """

    try:
        query_job = client.query(query)
        results = list(query_job.result())

        # Convertir en liste de dicts
        data = []
        for row in results:
            data.append(dict(row.items()))

        logger.info(f"query_all_prices: {len(data)} lignes retournees")
        return data

    except Exception as e:
        logger.error(f"Erreur query_all_prices: {e}")
        raise


def get_distinct_values(field: str, vendor: Optional[str] = None) -> List[str]:
    """
    Retourne les valeurs distinctes d'un champ.
    Utile pour voir les categories, methodes de peche, etc. existantes.

    Args:
        field: Nom du champ (categorie, methode_peche, etc.)
        vendor: Optionnel - filtrer par vendor

    Returns:
        Liste des valeurs distinctes (non-null)
    """
    client = get_bigquery_client()
    table_id = f"{client.project}.{DATASET_ID}.AllPrices"

    # Valider le nom du champ (securite)
    allowed_fields = [
        "categorie", "methode_peche", "qualite", "decoupe", "etat", "origine",
        "calibre", "type_production", "couleur",
        "conservation", "trim", "label", "variante", "vendor"
    ]

    if field not in allowed_fields:
        raise ValueError(f"Champ non autorise: {field}. Champs valides: {allowed_fields}")

    where_clause = ""
    if vendor:
        escaped_vendor = vendor.replace("'", "''")
        where_clause = f"WHERE vendor = '{escaped_vendor}'"

    query = f"""
    SELECT DISTINCT {field}
    FROM `{table_id}`
    {where_clause}
    WHERE {field} IS NOT NULL
    ORDER BY {field}
    """

    # Fixer le WHERE si vendor est present
    if vendor:
        query = f"""
        SELECT DISTINCT {field}
        FROM `{table_id}`
        WHERE vendor = '{vendor.replace("'", "''")}' AND {field} IS NOT NULL
        ORDER BY {field}
        """
    else:
        query = f"""
        SELECT DISTINCT {field}
        FROM `{table_id}`
        WHERE {field} IS NOT NULL
        ORDER BY {field}
        """

    try:
        query_job = client.query(query)
        results = list(query_job.result())
        return [row[field] for row in results]

    except Exception as e:
        logger.error(f"Erreur get_distinct_values({field}): {e}")
        raise


def count_by_field(field: str, vendor: Optional[str] = None, limit: int = 50) -> List[Dict[str, Any]]:
    """
    Compte les occurrences par valeur d'un champ.
    Utile pour voir la distribution des valeurs.

    Args:
        field: Nom du champ
        vendor: Optionnel - filtrer par vendor
        limit: Nombre max de valeurs (default: 50)

    Returns:
        Liste de {"value": str, "count": int} triee par count DESC
    """
    client = get_bigquery_client()
    table_id = f"{client.project}.{DATASET_ID}.AllPrices"

    # Valider le nom du champ (securite)
    allowed_fields = [
        "categorie", "methode_peche", "qualite", "decoupe", "etat", "origine",
        "calibre", "type_production", "couleur",
        "conservation", "trim", "label", "variante", "vendor"
    ]

    if field not in allowed_fields:
        raise ValueError(f"Champ non autorise: {field}. Champs valides: {allowed_fields}")

    where_clause = ""
    if vendor:
        escaped_vendor = vendor.replace("'", "''")
        where_clause = f"WHERE vendor = '{escaped_vendor}'"

    query = f"""
    SELECT
        COALESCE({field}, '(NULL)') as value,
        COUNT(*) as count
    FROM `{table_id}`
    {where_clause}
    GROUP BY {field}
    ORDER BY count DESC
    LIMIT {limit}
    """

    try:
        query_job = client.query(query)
        results = list(query_job.result())
        return [{"value": row.value, "count": row.count} for row in results]

    except Exception as e:
        logger.error(f"Erreur count_by_field({field}): {e}")
        raise


def get_total_count(vendor: Optional[str] = None) -> int:
    """
    Retourne le nombre total de lignes dans AllPrices.

    Args:
        vendor: Optionnel - filtrer par vendor

    Returns:
        Nombre total de lignes
    """
    client = get_bigquery_client()
    table_id = f"{client.project}.{DATASET_ID}.AllPrices"

    where_clause = ""
    if vendor:
        escaped_vendor = vendor.replace("'", "''")
        where_clause = f"WHERE vendor = '{escaped_vendor}'"

    query = f"""
    SELECT COUNT(*) as total
    FROM `{table_id}`
    {where_clause}
    """

    try:
        query_job = client.query(query)
        result = list(query_job.result())[0]
        return result.total

    except Exception as e:
        logger.error(f"Erreur get_total_count: {e}")
        raise


def get_date_range(vendor: Optional[str] = None) -> Dict[str, str]:
    """
    Retourne la plage de dates dans AllPrices.

    Args:
        vendor: Optionnel - filtrer par vendor

    Returns:
        {"min_date": "YYYY-MM-DD", "max_date": "YYYY-MM-DD"}
    """
    client = get_bigquery_client()
    table_id = f"{client.project}.{DATASET_ID}.AllPrices"

    where_clause = ""
    if vendor:
        escaped_vendor = vendor.replace("'", "''")
        where_clause = f"WHERE vendor = '{escaped_vendor}'"

    query = f"""
    SELECT
        MIN(date) as min_date,
        MAX(date) as max_date
    FROM `{table_id}`
    {where_clause}
    """

    try:
        query_job = client.query(query)
        result = list(query_job.result())[0]
        return {
            "min_date": str(result.min_date) if result.min_date else None,
            "max_date": str(result.max_date) if result.max_date else None
        }

    except Exception as e:
        logger.error(f"Erreur get_date_range: {e}")
        raise
