"""
Script pour charger UNIQUEMENT Demarne dans AllPrices avec logs détaillés.
"""
import sys
sys.path.insert(0, '/app')

import logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

from pathlib import Path
import uuid

def load_demarne_only():
    print("=" * 60)
    print("CHARGEMENT DEMARNE UNIQUEMENT")
    print("=" * 60)
    
    # 1. Trouver le fichier
    import subprocess
    result = subprocess.run(["find", "/app", "-name", "*.xlsx", "-type", "f"], 
                           capture_output=True, text=True)
    xlsx_files = [f for f in result.stdout.strip().split('\n') if f and 'Demarne' in f or 'Classeur' in f]
    
    if not xlsx_files:
        xlsx_files = [f for f in result.stdout.strip().split('\n') if f]
    
    if not xlsx_files:
        print("ERREUR: Aucun fichier Excel trouvé")
        return
        
    sample_file = Path(xlsx_files[0])
    print(f"\n[1] FICHIER: {sample_file}")
    
    with open(sample_file, "rb") as f:
        file_bytes = f.read()
    
    # 2. Parser avec harmonisation
    print("\n[2] PARSING")
    from parsers import demarne
    products = demarne.parse(file_bytes, harmonize=True, date_fallback="2026-01-15")
    print(f"    Produits parsés: {len(products)}")
    
    # 3. Appeler directement load_to_all_prices
    print("\n[3] CHARGEMENT DIRECT DANS ALLPRICES")
    from services.bigquery import load_to_all_prices
    
    job_id = str(uuid.uuid4())
    print(f"    Job ID: {job_id}")
    
    try:
        result = load_to_all_prices(job_id, "demarne", products)
        print(f"\n    SUCCÈS!")
        print(f"    Résultat: {result}")
    except Exception as e:
        print(f"\n    ÉCHEC!")
        print(f"    Erreur: {type(e).__name__}: {e}")
        
        # Afficher plus de détails
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    load_demarne_only()
