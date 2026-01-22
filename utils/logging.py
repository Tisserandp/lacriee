"""
Logging configuration module for LaCriee pipeline.

Provides centralized logging setup with support for both file and console output,
with safe handling of non-ASCII characters.
"""
import logging
import os
import re


LOG_DIR = "./logs"
LOG_PATH = os.path.join(LOG_DIR, "pdf_parser.log")


class SafeConsoleFormatter(logging.Formatter):
    """
    Custom formatter that safely handles non-printable characters.

    Removes surrogates and other problematic Unicode characters that can
    cause issues in terminal output.
    """
    def format(self, record):
        msg = super().format(record)
        # Supprime les caractères non imprimables (comme les surrogates / emojis)
        return re.sub(r'[\ud800-\udfff]', '', msg)


def setup_logging() -> logging.Logger:
    """
    Configure logging for the application.

    Sets up both file and console handlers with appropriate formatters.
    Ensures log directory exists and clears any existing handlers to avoid duplicates.

    Returns:
        logging.Logger: Configured root logger
    """
    # Créer le répertoire logs s'il n'existe pas
    os.makedirs(LOG_DIR, exist_ok=True)

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # Nettoyage des handlers existants pour éviter les doublons
    if logger.hasHandlers():
        logger.handlers.clear()

    # File handler UTF-8 (enregistrement complet)
    fh = logging.FileHandler(LOG_PATH, encoding="utf-8")
    fh.setLevel(logging.INFO)
    fh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

    # Console handler sans caractères problématiques
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(SafeConsoleFormatter('%(asctime)s - %(levelname)s - %(message)s'))

    # Enregistrement des handlers
    logger.addHandler(fh)
    logger.addHandler(ch)

    return logger


def get_logger() -> logging.Logger:
    """
    Get the configured logger instance.

    Returns:
        logging.Logger: The root logger (already configured by setup_logging())
    """
    return logging.getLogger()
