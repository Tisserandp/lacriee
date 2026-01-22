"""
Service centralisé d'extraction de dates.
Regroupe tous les patterns de date utilisés par les différents parsers.
"""
import re
from datetime import date, datetime
from typing import Optional
import logging

logger = logging.getLogger(__name__)

# Dictionnaire des mois français
MOIS_FR = {
    "janvier": 1, "février": 2, "fevrier": 2, "mars": 3, "avril": 4,
    "mai": 5, "juin": 6, "juillet": 7, "août": 8, "aout": 8,
    "septembre": 9, "octobre": 10, "novembre": 11, "décembre": 12, "decembre": 12
}


class DateExtractor:
    """
    Service centralisé pour extraire les dates depuis différents formats.
    """

    # Patterns de date supportés
    PATTERNS = {
        # Laurent-Daniel: "12 janvier 2024"
        "french_text": re.compile(r"(\d{1,2})\s+([a-zéûè]+)\s+(\d{4})", re.IGNORECASE),
        # Demarne, Hennequin: "12/01/2024"
        "dd_mm_yyyy_slash": re.compile(r"\b(\d{2})/(\d{2})/(\d{4})\b"),
        # VVQM: "12.01.2024"
        "dd_mm_yyyy_dot": re.compile(r"\b(\d{2})\.(\d{2})\.(\d{4})\b"),
        # ISO: "2024-01-12"
        "iso": re.compile(r"\b(\d{4})-(\d{2})-(\d{2})\b"),
    }

    @classmethod
    def from_french_text(cls, text: str) -> Optional[str]:
        """
        Extrait une date au format français textuel.
        Ex: "12 janvier 2024" -> "2024-01-12"

        Args:
            text: Texte contenant potentiellement une date française

        Returns:
            Date au format ISO (YYYY-MM-DD) ou None
        """
        if not text:
            return None

        match = cls.PATTERNS["french_text"].search(text)
        if match:
            jour, mois_str, annee = match.groups()
            mois = MOIS_FR.get(mois_str.lower())
            if mois:
                try:
                    date_obj = date(int(annee), mois, int(jour))
                    return date_obj.isoformat()
                except ValueError:
                    pass
        return None

    @classmethod
    def from_dd_mm_yyyy(cls, text: str, separator: str = "/") -> Optional[str]:
        """
        Extrait une date au format DD/MM/YYYY ou DD.MM.YYYY.

        Args:
            text: Texte contenant potentiellement une date
            separator: Séparateur utilisé ("/" ou ".")

        Returns:
            Date au format ISO (YYYY-MM-DD) ou None
        """
        if not text:
            return None

        pattern_key = "dd_mm_yyyy_slash" if separator == "/" else "dd_mm_yyyy_dot"
        match = cls.PATTERNS[pattern_key].search(text)
        if match:
            jour, mois, annee = match.groups()
            try:
                date_obj = date(int(annee), int(mois), int(jour))
                return date_obj.isoformat()
            except ValueError:
                pass
        return None

    @classmethod
    def from_iso(cls, text: str) -> Optional[str]:
        """
        Extrait une date au format ISO YYYY-MM-DD.

        Args:
            text: Texte contenant potentiellement une date ISO

        Returns:
            Date au format ISO (YYYY-MM-DD) ou None
        """
        if not text:
            return None

        match = cls.PATTERNS["iso"].search(text)
        if match:
            annee, mois, jour = match.groups()
            try:
                date_obj = date(int(annee), int(mois), int(jour))
                return date_obj.isoformat()
            except ValueError:
                pass
        return None

    @classmethod
    def extract(cls, text: str, vendor: Optional[str] = None) -> Optional[str]:
        """
        Extrait une date en essayant tous les patterns.
        Si un vendor est spécifié, utilise son pattern en priorité.

        Args:
            text: Texte contenant potentiellement une date
            vendor: Nom du vendor (optionnel, pour prioriser le pattern)

        Returns:
            Date au format ISO (YYYY-MM-DD) ou None
        """
        if not text:
            return None

        # Ordre de priorité par vendor
        vendor_priority = {
            "laurent_daniel": ["french_text", "dd_mm_yyyy_slash", "dd_mm_yyyy_dot", "iso"],
            "demarne": ["dd_mm_yyyy_slash", "iso", "french_text", "dd_mm_yyyy_dot"],
            "vvqm": ["dd_mm_yyyy_dot", "dd_mm_yyyy_slash", "iso", "french_text"],
            "hennequin": ["dd_mm_yyyy_slash", "iso", "french_text", "dd_mm_yyyy_dot"],
        }

        # Déterminer l'ordre des patterns à essayer
        if vendor and vendor.lower() in vendor_priority:
            patterns_order = vendor_priority[vendor.lower()]
        else:
            patterns_order = ["dd_mm_yyyy_slash", "french_text", "dd_mm_yyyy_dot", "iso"]

        # Essayer chaque pattern dans l'ordre
        for pattern_name in patterns_order:
            if pattern_name == "french_text":
                result = cls.from_french_text(text)
            elif pattern_name == "dd_mm_yyyy_slash":
                result = cls.from_dd_mm_yyyy(text, "/")
            elif pattern_name == "dd_mm_yyyy_dot":
                result = cls.from_dd_mm_yyyy(text, ".")
            elif pattern_name == "iso":
                result = cls.from_iso(text)
            else:
                continue

            if result:
                return result

        return None

    @classmethod
    def parse_fallback(cls, date_fallback: str) -> Optional[str]:
        """
        Parse une date de fallback fournie par l'utilisateur.
        Accepte les formats: YYYY-MM-DD, DD/MM/YYYY, DD.MM.YYYY

        Args:
            date_fallback: Date fournie par l'utilisateur

        Returns:
            Date au format ISO (YYYY-MM-DD)

        Raises:
            ValueError: Si le format est invalide
        """
        if not date_fallback:
            return None

        # Essayer le format ISO d'abord
        result = cls.from_iso(date_fallback)
        if result:
            return result

        # Essayer DD/MM/YYYY
        result = cls.from_dd_mm_yyyy(date_fallback, "/")
        if result:
            return result

        # Essayer DD.MM.YYYY
        result = cls.from_dd_mm_yyyy(date_fallback, ".")
        if result:
            return result

        raise ValueError(
            f"Format de date invalide: '{date_fallback}'. "
            "Formats acceptés: YYYY-MM-DD, DD/MM/YYYY, DD.MM.YYYY"
        )
