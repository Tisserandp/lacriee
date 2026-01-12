"""
Test direct du pipeline ELT sans passer par l'API HTTP.
"""
import asyncio
import sys
from pathlib import Path
import logging

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


async def test_pipeline():
    """Test complet du pipeline."""
    from main import extract_LD_data_from_pdf
    from services.import_service import ImportService
    
    def parse_laurent_daniel(file_bytes: bytes, **kwargs) -> list[dict]:
        return extract_LD_data_from_pdf(file_bytes)
    
    # Créer le service
    service = ImportService("laurent_daniel", parse_laurent_daniel)
    
    # Lire le fichier de test
    sample_file = Path("Samples/LaurentD/CC.pdf")
    if not sample_file.exists():
        logger.error(f"Fichier non trouvé: {sample_file}")
        return False
    
    with open(sample_file, "rb") as f:
        file_bytes = f.read()
    
    logger.info(f"✅ Fichier chargé: {sample_file} ({len(file_bytes)} bytes)")
    
    try:
        # 1. Partie synchrone
        logger.info("📤 Partie synchrone (archivage + création job)...")
        result = service.process_sync(
            filename=sample_file.name,
            file_bytes=file_bytes,
            file_size=len(file_bytes)
        )
        
        if result.get("status") != "processing":
            logger.error(f"❌ Échec partie synchrone: {result.get('error')}")
            return False
        
        job_id = result.get("job_id")
        logger.info(f"✅ Job créé: {job_id}")
        logger.info(f"   GCS URL: {result.get('gcs_url')}")
        
        # 2. Partie asynchrone
        logger.info("🔄 Partie asynchrone (parsing + staging + transform)...")
        await service.process_async(job_id, file_bytes)
        
        # 3. Vérifier le résultat
        logger.info("🔍 Vérification du résultat...")
        from services.bigquery import get_job_status
        job = get_job_status(job_id)
        
        if not job:
            logger.error("❌ Job non trouvé dans BigQuery")
            return False
        
        logger.info(f"✅ Statut final: {job.get('status')}")
        logger.info(f"   Rows extracted: {job.get('rows_extracted')}")
        logger.info(f"   Rows loaded staging: {job.get('rows_loaded_staging')}")
        logger.info(f"   Rows inserted prod: {job.get('rows_inserted_prod')}")
        logger.info(f"   Rows updated prod: {job.get('rows_updated_prod')}")
        logger.info(f"   Unknown products: {job.get('rows_unknown_products')}")
        logger.info(f"   Duration: {job.get('duration_seconds')}s")
        
        if job.get('status') == 'completed':
            logger.info("🎉 Pipeline terminé avec succès!")
            return True
        else:
            logger.error(f"❌ Pipeline échoué: {job.get('error_message')}")
            return False
            
    except Exception as e:
        logger.exception(f"❌ Erreur: {e}")
        return False


if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info("=== Test Direct du Pipeline ELT ===")
    logger.info("=" * 60)
    
    try:
        success = asyncio.run(test_pipeline())
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        logger.info("\n⚠️ Test interrompu")
        sys.exit(1)
    except Exception as e:
        logger.exception(f"❌ Erreur fatale: {e}")
        sys.exit(1)

