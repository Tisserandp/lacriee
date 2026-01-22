"""
Parsers module pour extraction de données depuis PDF/Excel.

Ce module fournit des parseurs pour les différents fournisseurs de produits marée:
- Audierne: Viviers d'Audierne (PDF)
- Hennequin: Marée Hennequin (PDF)
- VVQM: Viviers et Vrac Qualité Marée (PDF)
- Laurent Daniel: LD (PDF)
- Demarne: (Excel)

Chaque parseur supporte l'harmonisation des attributs via le paramètre `harmonize=True`.
Voir docs/harmonisation_attributs.md pour les règles de normalisation.

Usage:
    from parsers import audierne, hennequin, vvqm, laurent_daniel, demarne

    # Sans harmonisation (données brutes)
    products = audierne.parse(file_bytes)

    # Avec harmonisation (attributs normalisés)
    products = audierne.parse(file_bytes, harmonize=True)

    # Demarne (Excel) - accepte chemin fichier ou bytes
    products = demarne.parse("cours.xlsx", harmonize=True, date_fallback="2026-01-15")
"""

from parsers import audierne
from parsers import hennequin
from parsers import vvqm
from parsers import laurent_daniel
from parsers import demarne

__all__ = [
    "audierne",
    "hennequin",
    "vvqm",
    "laurent_daniel",
    "demarne",
]
