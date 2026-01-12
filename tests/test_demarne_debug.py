"""Test debug pour Demarne."""
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from main import parse_demarne_excel_data
import traceback

try:
    file_path = "Samples/Demarne/Classeur1 G19.xlsx"
    print(f"Test avec: {file_path}")
    
    result = parse_demarne_excel_data(file_path, date_fallback="2026-01-12")
    print(f"✅ Parsing réussi: {len(result)} lignes")
    print(f"Premier élément: {result[0] if result else 'Aucun'}")
    
except Exception as e:
    print(f"❌ Erreur: {e}")
    traceback.print_exc()
