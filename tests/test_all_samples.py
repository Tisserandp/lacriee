"""
Test de bout en bout avec tous les fichiers Samples.
"""
import asyncio
import sys
from pathlib import Path
import logging

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


async def test_laurent_daniel():
    """Test avec le fichier Laurent-Daniel."""
    from main import extract_LD_data_from_pdf
    from services.import_service import ImportService
    
    def parse_laurent_daniel(file_bytes: bytes, **kwargs) -> list[dict]:
        return extract_LD_data_from_pdf(file_bytes)
    
    service = ImportService("laurent_daniel", parse_laurent_daniel)
    sample_file = Path("Samples/LaurentD/CC.pdf")
    
    if not sample_file.exists():
        logger.error(f"Fichier non trouvé: {sample_file}")
        return False
    
    with open(sample_file, "rb") as f:
        file_bytes = f.read()
    
    logger.info(f"📄 Test Laurent-Daniel: {sample_file.name}")
    
    try:
        result = service.process_sync(sample_file.name, file_bytes, len(file_bytes))
        if result.get("status") != "processing":
            logger.error(f"❌ Échec: {result.get('error')}")
            return False
        
        job_id = result.get("job_id")
        logger.info(f"   Job ID: {job_id}")
        
        await service.process_async(job_id, file_bytes)
        
        # Attendre un peu pour que le streaming buffer se vide et que le statut soit mis à jour
        import asyncio
        await asyncio.sleep(15)
        
        from services.bigquery import get_job_status
        job = get_job_status(job_id)
        
        if job:
            status = job.get('status')
            rows_extracted = job.get('rows_extracted')
            rows_inserted = job.get('rows_inserted_prod')
            
            # Vérifier si les données sont présentes dans BigQuery directement
            from services.bigquery import get_bigquery_client, DATASET_ID
            client = get_bigquery_client()
            count_query = f"""
            SELECT COUNT(*) AS total_rows
            FROM `{client.project}.{DATASET_ID}.ProvidersPrices`
            WHERE job_id = '{job_id}'
            """
            count_result = list(client.query(count_query).result())
            db_rows = count_result[0].total_rows if count_result else 0
            
            if status == 'completed' or (rows_extracted and rows_extracted > 0) or db_rows > 0:
                logger.info(f"   ✅ Succès: {rows_extracted or db_rows} lignes extraites, {rows_inserted or db_rows} insérées (statut: {status})")
                return True
            elif status == 'failed':
                logger.error(f"   ❌ Échec: {job.get('error_message')}")
                return False
            else:
                logger.warning(f"   ⚠️ Statut: {status}, pas encore de données")
                return False
        else:
            logger.error(f"   ❌ Job non trouvé")
            return False
    except Exception as e:
        logger.exception(f"❌ Erreur: {e}")
        return False


async def test_vvqm():
    """Test avec le fichier VVQM."""
    from main import parse_vvq_pdf_data, sanitize_for_json
    from services.import_service import ImportService
    
    def parse_vvqm(file_bytes: bytes, **kwargs) -> list[dict]:
        df = parse_vvq_pdf_data(file_bytes)
        return sanitize_for_json(df[["keyDate", "Vendor", "ProductName", "Code_Provider", "Date", "Prix", "Categorie"]])
    
    service = ImportService("vvqm", parse_vvqm)
    # Essayer plusieurs fichiers possibles
    sample_files = [
        Path("Samples/VVQ/GEXPORT.pdf"),
        Path("Samples/VVQM/GEXPORT.pdf"),
        Path("Samples/VVQ/vvqm.pdf"),
        Path("Samples/VVQM/vvqm.pdf"),
    ]
    sample_file = None
    for f in sample_files:
        if f.exists():
            sample_file = f
            break
    
    if not sample_file:
        logger.warning(f"⚠️ Aucun fichier VVQM trouvé dans Samples/VVQ ou Samples/VVQM")
        return None
    
    with open(sample_file, "rb") as f:
        file_bytes = f.read()
    
    logger.info(f"📄 Test VVQM: {sample_file.name}")
    
    try:
        result = service.process_sync(sample_file.name, file_bytes, len(file_bytes))
        if result.get("status") != "processing":
            logger.error(f"❌ Échec: {result.get('error')}")
            return False
        
        job_id = result.get("job_id")
        logger.info(f"   Job ID: {job_id}")
        
        await service.process_async(job_id, file_bytes)
        
        # Attendre un peu pour que le streaming buffer se vide et que le statut soit mis à jour
        import asyncio
        await asyncio.sleep(15)
        
        from services.bigquery import get_job_status
        job = get_job_status(job_id)
        
        if job:
            status = job.get('status')
            rows_extracted = job.get('rows_extracted')
            rows_inserted = job.get('rows_inserted_prod')
            
            # Vérifier si les données sont présentes dans BigQuery directement
            from services.bigquery import get_bigquery_client, DATASET_ID
            client = get_bigquery_client()
            count_query = f"""
            SELECT COUNT(*) AS total_rows
            FROM `{client.project}.{DATASET_ID}.ProvidersPrices`
            WHERE job_id = '{job_id}'
            """
            count_result = list(client.query(count_query).result())
            db_rows = count_result[0].total_rows if count_result else 0
            
            if status == 'completed' or (rows_extracted and rows_extracted > 0) or db_rows > 0:
                logger.info(f"   ✅ Succès: {rows_extracted or db_rows} lignes extraites, {rows_inserted or db_rows} insérées (statut: {status})")
                return True
            elif status == 'failed':
                logger.error(f"   ❌ Échec: {job.get('error_message')}")
                return False
            else:
                logger.warning(f"   ⚠️ Statut: {status}, pas encore de données")
                return False
        else:
            logger.error(f"   ❌ Job non trouvé")
            return False
    except Exception as e:
        logger.exception(f"❌ Erreur: {e}")
        return False


async def test_demarne():
    """Test avec le fichier Demarne."""
    from main import parse_demarne_excel_data
    from services.import_service import ImportService
    import tempfile
    import os
    
    def parse_demarne(file_bytes: bytes, date_fallback: str = None, **kwargs) -> list[dict]:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
            tmp.write(file_bytes)
            tmp_path = tmp.name
        try:
            return parse_demarne_excel_data(tmp_path, date_fallback=date_fallback)
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
    
    service = ImportService("demarne", parse_demarne)
    # Essayer plusieurs fichiers possibles
    sample_files = [
        Path("Samples/Demarne/Classeur1 G19.xlsx"),
        Path("Samples/Demarne/Classeur_error.xlsx"),
        Path("Samples/Demarne/demarne.xlsx"),
        Path("Samples/Demarne/demarne.xls"),
    ]
    sample_file = None
    for f in sample_files:
        if f.exists():
            sample_file = f
            break
    
    if not sample_file:
        logger.warning(f"⚠️ Aucun fichier Demarne trouvé dans Samples/Demarne")
        return None
    
    with open(sample_file, "rb") as f:
        file_bytes = f.read()
    
    logger.info(f"📄 Test Demarne: {sample_file.name}")
    
    try:
        # Demarne nécessite une date de fallback
        result = service.process_sync(sample_file.name, file_bytes, len(file_bytes))
        if result.get("status") != "processing":
            logger.error(f"❌ Échec: {result.get('error')}")
            return False
        
        job_id = result.get("job_id")
        logger.info(f"   Job ID: {job_id}")
        
        # Utiliser une date de fallback pour le test
        await service.process_async(job_id, file_bytes, parser_kwargs={"date_fallback": "2026-01-12"})
        
        from services.bigquery import get_job_status
        job = get_job_status(job_id)
        
        if job and job.get('status') == 'completed':
            logger.info(f"   ✅ Succès: {job.get('rows_extracted')} lignes extraites")
            return True
        else:
            logger.error(f"   ❌ Échec: {job.get('error_message') if job else 'Job non trouvé'}")
            return False
    except Exception as e:
        logger.exception(f"❌ Erreur: {e}")
        return False


async def test_all():
    """Test tous les fichiers Samples."""
    logger.info("=" * 60)
    logger.info("=== Tests de Bout en Bout - Tous les Samples ===")
    logger.info("=" * 60)
    
    results = {}
    
    # Test Laurent-Daniel
    logger.info("\n" + "=" * 60)
    results['laurent_daniel'] = await test_laurent_daniel()
    
    # Test VVQM
    logger.info("\n" + "=" * 60)
    results['vvqm'] = await test_vvqm()
    
    # Test Demarne
    logger.info("\n" + "=" * 60)
    results['demarne'] = await test_demarne()
    
    # Résumé
    logger.info("\n" + "=" * 60)
    logger.info("=== RÉSUMÉ DES TESTS ===")
    logger.info("=" * 60)
    
    for vendor, result in results.items():
        if result is None:
            logger.info(f"{vendor}: ⚠️ Non testé (fichier manquant)")
        elif result:
            logger.info(f"{vendor}: ✅ Succès")
        else:
            logger.info(f"{vendor}: ❌ Échec")
    
    success_count = sum(1 for r in results.values() if r is True)
    total_count = sum(1 for r in results.values() if r is not None)
    
    logger.info(f"\nTotal: {success_count}/{total_count} tests réussis")
    
    return success_count == total_count


if __name__ == "__main__":
    try:
        success = asyncio.run(test_all())
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        logger.info("\n⚠️ Tests interrompus")
        sys.exit(1)
    except Exception as e:
        logger.exception(f"❌ Erreur fatale: {e}")
        sys.exit(1)
