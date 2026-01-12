"""Test debug pour VVQM."""
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from main import parse_vvq_pdf_data, sanitize_for_json
import traceback

try:
    with open("Samples/VVQ/GEXPORT.pdf", "rb") as f:
        file_bytes = f.read()
    
    print(f"Fichier lu: {len(file_bytes)} bytes")
    
    df = parse_vvq_pdf_data(file_bytes)
    print(f"✅ Parsing réussi: {len(df)} lignes")
    print(f"Colonnes: {list(df.columns)}")
    
    result = sanitize_for_json(df[["keyDate", "Vendor", "ProductName", "Code_Provider", "Date", "Prix", "Categorie"]])
    print(f"✅ Conversion JSON: {len(result)} éléments")
    
except Exception as e:
    print(f"❌ Erreur: {e}")
    traceback.print_exc()
