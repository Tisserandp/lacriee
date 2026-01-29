"""
Service d'analyse qualite pour les donnees AllPrices.
Permet d'identifier les problemes de qualite et les ameliorations a apporter aux parseurs.
"""
import logging
from typing import Dict, Any, Optional, List
from .bigquery import get_bigquery_client, DATASET_ID
from .data_query import get_total_count, get_date_range, count_by_field

logger = logging.getLogger(__name__)

# Champs harmonises a analyser
HARMONIZED_FIELDS = [
    "categorie", "methode_peche", "qualite", "decoupe", "etat", "origine",
    "calibre", "type_production", "technique_abattage", "couleur",
    "conservation", "trim", "label", "variante"
]


def analyze_field_coverage(vendor: Optional[str] = None) -> Dict[str, float]:
    """
    Calcule le pourcentage de remplissage pour chaque champ harmonise.

    Args:
        vendor: Optionnel - filtrer par vendor

    Returns:
        Dict {field_name: percentage} trie par pourcentage decroissant
        Exemple: {"categorie": 98.5, "methode_peche": 45.2, "qualite": 23.1, ...}
    """
    client = get_bigquery_client()
    table_id = f"{client.project}.{DATASET_ID}.AllPrices"

    where_clause = ""
    if vendor:
        escaped_vendor = vendor.replace("'", "''")
        where_clause = f"WHERE vendor = '{escaped_vendor}'"

    # Construire une requete qui compte les non-null pour chaque champ
    count_expressions = []
    for field in HARMONIZED_FIELDS:
        count_expressions.append(f"COUNTIF({field} IS NOT NULL) as {field}_filled")

    query = f"""
    SELECT
        COUNT(*) as total,
        {', '.join(count_expressions)}
    FROM `{table_id}`
    {where_clause}
    """

    try:
        query_job = client.query(query)
        result = list(query_job.result())[0]

        total = result.total
        if total == 0:
            return {field: 0.0 for field in HARMONIZED_FIELDS}

        coverage = {}
        for field in HARMONIZED_FIELDS:
            filled = getattr(result, f"{field}_filled")
            coverage[field] = round((filled / total) * 100, 1)

        # Trier par pourcentage decroissant
        coverage = dict(sorted(coverage.items(), key=lambda x: -x[1]))

        logger.info(f"analyze_field_coverage: total={total}, vendor={vendor}")
        return coverage

    except Exception as e:
        logger.error(f"Erreur analyze_field_coverage: {e}")
        raise


def find_null_values_sample(field: str, vendor: Optional[str] = None, limit: int = 10) -> List[Dict[str, Any]]:
    """
    Retourne des exemples de produits ou un champ est NULL.
    Utile pour comprendre pourquoi certains champs ne sont pas remplis.

    Args:
        field: Champ a analyser
        vendor: Optionnel - filtrer par vendor
        limit: Nombre d'exemples (default: 10)

    Returns:
        Liste de produits avec le champ NULL
    """
    client = get_bigquery_client()
    table_id = f"{client.project}.{DATASET_ID}.AllPrices"

    # Valider le champ
    if field not in HARMONIZED_FIELDS:
        raise ValueError(f"Champ non autorise: {field}")

    where_parts = [f"{field} IS NULL"]
    if vendor:
        escaped_vendor = vendor.replace("'", "''")
        where_parts.append(f"vendor = '{escaped_vendor}'")

    where_clause = "WHERE " + " AND ".join(where_parts)

    query = f"""
    SELECT
        key_date, date, vendor, code_provider, product_name, prix,
        categorie, methode_peche, qualite, decoupe, etat, origine, calibre
    FROM `{table_id}`
    {where_clause}
    ORDER BY date DESC
    LIMIT {limit}
    """

    try:
        query_job = client.query(query)
        results = list(query_job.result())
        return [dict(row.items()) for row in results]

    except Exception as e:
        logger.error(f"Erreur find_null_values_sample({field}): {e}")
        raise


def get_quality_summary(vendor: str) -> Dict[str, Any]:
    """
    Resume qualite complet pour un vendor.

    Args:
        vendor: Nom du vendor

    Returns:
        {
            "vendor": str,
            "total_records": int,
            "date_range": {"min_date": str, "max_date": str},
            "field_coverage": {field: percentage, ...},
            "top_categories": [{value, count}, ...],
            "low_coverage_fields": [field, ...]  # champs < 50%
        }
    """
    try:
        total = get_total_count(vendor=vendor)
        dates = get_date_range(vendor=vendor)
        coverage = analyze_field_coverage(vendor=vendor)
        top_cats = count_by_field("categorie", vendor=vendor, limit=10)

        # Identifier les champs avec faible couverture (< 50%)
        low_coverage = [f for f, pct in coverage.items() if pct < 50]

        return {
            "vendor": vendor,
            "total_records": total,
            "date_range": dates,
            "field_coverage": coverage,
            "top_categories": top_cats,
            "low_coverage_fields": low_coverage
        }

    except Exception as e:
        logger.error(f"Erreur get_quality_summary({vendor}): {e}")
        raise


def find_potential_harmonization_issues(vendor: Optional[str] = None) -> Dict[str, List[Dict]]:
    """
    Identifie les valeurs potentiellement problematiques dans les champs harmonises.
    Cherche les valeurs qui ressemblent a du texte brut non normalise.

    Args:
        vendor: Optionnel - filtrer par vendor

    Returns:
        {
            "categorie": [{"value": str, "count": int, "issue": str}, ...],
            ...
        }
    """
    issues = {}

    for field in ["categorie", "methode_peche", "qualite", "etat", "origine"]:
        values = count_by_field(field, vendor=vendor, limit=100)

        field_issues = []
        for v in values:
            value = v["value"]
            count = v["count"]

            if value == "(NULL)":
                continue

            # Detecter les problemes potentiels
            issue = None

            # Valeur avec accents (devrait etre normalise)
            if any(c in value for c in "éèêëàâäùûüôöîïç"):
                issue = "contient des accents"

            # Valeur en minuscules
            elif value != value.upper():
                issue = "n'est pas en majuscules"

            # Valeur trop longue (peut-etre du texte brut)
            elif len(value) > 30:
                issue = "valeur trop longue"

            if issue:
                field_issues.append({
                    "value": value,
                    "count": count,
                    "issue": issue
                })

        if field_issues:
            issues[field] = field_issues

    return issues


def compare_vendors() -> List[Dict[str, Any]]:
    """
    Compare les statistiques entre vendors.

    Returns:
        Liste de stats par vendor triee par total_records
    """
    client = get_bigquery_client()
    table_id = f"{client.project}.{DATASET_ID}.AllPrices"

    query = f"""
    SELECT
        vendor,
        COUNT(*) as total_records,
        MIN(date) as first_date,
        MAX(date) as last_date,
        COUNT(DISTINCT date) as distinct_dates,
        COUNTIF(categorie IS NOT NULL) as with_categorie,
        COUNTIF(methode_peche IS NOT NULL) as with_methode_peche,
        COUNTIF(qualite IS NOT NULL) as with_qualite
    FROM `{table_id}`
    GROUP BY vendor
    ORDER BY total_records DESC
    """

    try:
        query_job = client.query(query)
        results = list(query_job.result())

        vendors = []
        for row in results:
            total = row.total_records
            vendors.append({
                "vendor": row.vendor,
                "total_records": total,
                "first_date": str(row.first_date) if row.first_date else None,
                "last_date": str(row.last_date) if row.last_date else None,
                "distinct_dates": row.distinct_dates,
                "categorie_pct": round((row.with_categorie / total) * 100, 1) if total > 0 else 0,
                "methode_peche_pct": round((row.with_methode_peche / total) * 100, 1) if total > 0 else 0,
                "qualite_pct": round((row.with_qualite / total) * 100, 1) if total > 0 else 0
            })

        return vendors

    except Exception as e:
        logger.error(f"Erreur compare_vendors: {e}")
        raise
