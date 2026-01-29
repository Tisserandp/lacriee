"""
Script pour lister TOUTES les categories par vendor.
Filtre sur date >= 2026-01-26 (donnees recentes uniquement).
"""
import logging
from services.bigquery import get_bigquery_client, DATASET_ID

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

RECENT_DATE = "2026-01-26"


def list_all_categories():
    """Liste toutes les categories par vendor."""

    print("\n" + "="*100)
    print("LISTE COMPLETE DES CATEGORIES PAR VENDOR")
    print(f"(Donnees >= {RECENT_DATE})")
    print("="*100)

    vendors = ["Demarne", "Audierne", "Laurent Daniel", "Hennequin", "VVQM"]
    client = get_bigquery_client()
    table_id = f"{client.project}.{DATASET_ID}.AllPrices"

    all_categories_by_vendor = {}

    for vendor in vendors:
        print(f"\n{'='*100}")
        print(f"Vendor: {vendor}")
        print(f"{'='*100}\n")

        escaped_vendor = vendor.replace("'", "''")

        query = f"""
        SELECT
            categorie,
            COUNT(*) as count
        FROM `{table_id}`
        WHERE vendor = '{escaped_vendor}' AND date >= '{RECENT_DATE}'
        GROUP BY categorie
        ORDER BY count DESC, categorie
        """

        try:
            query_job = client.query(query)
            results = list(query_job.result())

            categories = []
            total = 0

            for row in results:
                cat = row.categorie if row.categorie else "(NULL)"
                count = row.count
                total += count
                categories.append({"categorie": cat, "count": count})

            all_categories_by_vendor[vendor] = categories

            # Afficher avec formatage
            print(f"{'#':<4} {'Categorie':<40} {'Count':>10} {'%':>10}")
            print("-" * 70)

            for idx, cat in enumerate(categories, 1):
                pct = (cat["count"] / total) * 100 if total > 0 else 0
                cat_name = cat["categorie"][:40]
                print(f"{idx:<4} {cat_name:<40} {cat['count']:>10} {pct:>9.2f}%")

            print(f"\n{'TOTAL':<45} {total:>10}")

        except Exception as e:
            logger.error(f"Erreur pour {vendor}: {e}")
            raise

    # Afficher un resume CSV
    print(f"\n\n{'='*100}")
    print("EXPORT CSV - TOUTES LES CATEGORIES")
    print(f"{'='*100}\n")

    print("vendor,categorie,count")
    for vendor, categories in all_categories_by_vendor.items():
        for cat in categories:
            cat_name = cat["categorie"].replace('"', '""')  # Escape quotes for CSV
            print(f'"{vendor}","{cat_name}",{cat["count"]}')


if __name__ == "__main__":
    try:
        list_all_categories()
    except Exception as e:
        logger.error(f"Erreur lors de l'execution: {e}")
        raise
