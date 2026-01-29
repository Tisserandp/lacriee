"""
Script d'analyse des categories trop generiques dans AllPrices.
Identifie les categories qui ne sont pas assez specifiques.

IMPORTANT: Filtre sur date >= 2026-01-26 pour eviter l'historique charge
avec harmonisation minimale.
"""
import logging
from services.data_query import count_by_field, query_all_prices
from services.quality_analysis import compare_vendors
from services.bigquery import get_bigquery_client, DATASET_ID
from collections import defaultdict
from datetime import date, timedelta

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Categories considerees comme trop generiques
GENERIC_CATEGORIES = [
    "POISSON",
    "COQUILLAGE",
    "CRUSTACE",
    "FRUITS DE MER",
    "MOLLUSQUE",
    "FILET",
    "ENTIER",
    "PREPARATION",
    "SURIMI"
]

# Date de filtrage (donnees recentes uniquement)
RECENT_DATE = "2026-01-26"


def count_by_field_recent(field: str, vendor: str, date_from: str, limit: int = 100):
    """
    Compte les occurrences par valeur d'un champ avec filtre de date.
    Similaire a count_by_field mais avec date_from.
    """
    client = get_bigquery_client()
    table_id = f"{client.project}.{DATASET_ID}.AllPrices"

    escaped_vendor = vendor.replace("'", "''")
    escaped_date = date_from.replace("'", "''")

    query = f"""
    SELECT
        COALESCE({field}, '(NULL)') as value,
        COUNT(*) as count
    FROM `{table_id}`
    WHERE vendor = '{escaped_vendor}' AND date >= '{escaped_date}'
    GROUP BY {field}
    ORDER BY count DESC
    LIMIT {limit}
    """

    query_job = client.query(query)
    results = list(query_job.result())
    return [{"value": row.value, "count": row.count} for row in results]


def analyze_generic_categories():
    """Analyse les categories trop generiques par vendor."""

    print("\n" + "="*80)
    print("ANALYSE DES CATEGORIES TROP GENERIQUES")
    print(f"(Donnees >= {RECENT_DATE})")
    print("="*80)

    vendors = ["Demarne", "Audierne", "Laurent Daniel", "Hennequin", "VVQM"]

    results = {}

    for vendor in vendors:
        print(f"\n{'='*80}")
        print(f"Vendor: {vendor}")
        print(f"{'='*80}\n")

        # Obtenir la distribution des categories (DONNEES RECENTES UNIQUEMENT)
        categories = count_by_field_recent("categorie", vendor=vendor, date_from=RECENT_DATE, limit=100)

        total_products = sum(cat["count"] for cat in categories)
        generic_count = 0
        generic_details = []

        print(f"Total produits: {total_products}")
        print(f"\nDistribution des categories:")
        print(f"{'Categorie':<30} {'Count':>8} {'%':>8}")
        print("-" * 50)

        for cat in categories:
            value = cat["value"]
            count = cat["count"]
            pct = (count / total_products) * 100 if total_products > 0 else 0

            is_generic = value in GENERIC_CATEGORIES
            marker = " ⚠️ GENERIQUE" if is_generic else ""

            print(f"{value:<30} {count:>8} {pct:>7.1f}%{marker}")

            if is_generic:
                generic_count += count
                generic_details.append({
                    "categorie": value,
                    "count": count,
                    "pct": pct
                })

        generic_pct = (generic_count / total_products) * 100 if total_products > 0 else 0

        print(f"\n{'='*50}")
        print(f"TOTAL PRODUITS GENERIQUES: {generic_count}/{total_products} ({generic_pct:.1f}%)")
        print(f"{'='*50}")

        results[vendor] = {
            "total_products": total_products,
            "generic_count": generic_count,
            "generic_pct": generic_pct,
            "generic_details": generic_details
        }

        # Montrer des exemples de produits avec categories generiques
        if generic_count > 0:
            print(f"\nExemples de produits avec categories generiques:")
            print("-" * 80)

            for detail in generic_details[:3]:  # Top 3 categories generiques
                cat = detail["categorie"]
                examples = query_all_prices(
                    vendor=vendor,
                    categorie=cat,
                    date_from=RECENT_DATE,
                    limit=3
                )

                if examples:
                    print(f"\n  Categorie: {cat} ({detail['count']} produits)")
                    for ex in examples:
                        print(f"    - {ex['product_name']}")
                        if ex.get('methode_peche'):
                            print(f"      Methode: {ex['methode_peche']}")
                        if ex.get('decoupe'):
                            print(f"      Decoupe: {ex['decoupe']}")

    # Resume global
    print(f"\n\n{'='*80}")
    print("RESUME GLOBAL")
    print(f"{'='*80}\n")
    print(f"{'Vendor':<20} {'Total':>10} {'Generiques':>12} {'% Generic':>12}")
    print("-" * 80)

    for vendor, data in results.items():
        print(f"{vendor:<20} {data['total_products']:>10} {data['generic_count']:>12} {data['generic_pct']:>11.1f}%")

    # Recommandations
    print(f"\n\n{'='*80}")
    print("RECOMMANDATIONS")
    print(f"{'='*80}\n")

    for vendor, data in results.items():
        if data['generic_pct'] > 5:
            print(f"\n🔴 {vendor} - {data['generic_pct']:.1f}% de categories generiques")
            print(f"   Action: Ameliorer les mappings dans harmonize.py")
            print(f"   Categories a traiter:")
            for detail in data['generic_details']:
                if detail['pct'] > 2:
                    print(f"      - {detail['categorie']}: {detail['count']} produits ({detail['pct']:.1f}%)")
        elif data['generic_pct'] > 1:
            print(f"\n🟡 {vendor} - {data['generic_pct']:.1f}% de categories generiques")
            print(f"   Niveau acceptable mais ameliorable")
        else:
            print(f"\n🟢 {vendor} - {data['generic_pct']:.1f}% de categories generiques")
            print(f"   Excellent niveau de specificite")


if __name__ == "__main__":
    try:
        analyze_generic_categories()
    except Exception as e:
        logger.error(f"Erreur lors de l'analyse: {e}")
        raise
