from google.cloud import bigquery
from services.bigquery import get_bigquery_client, DATASET_ID
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def clear_table():
    client = get_bigquery_client()
    table_id = f"{client.project}.{DATASET_ID}.AllPrices"
    
    query = f"TRUNCATE TABLE `{table_id}`"
    
    try:
        logger.info(f"Clearing table {table_id}...")
        query_job = client.query(query)
        query_job.result()
        logger.info("Table cleared successfully.")
    except Exception as e:
        logger.error(f"Failed to clear table: {e}")

if __name__ == "__main__":
    clear_table()
