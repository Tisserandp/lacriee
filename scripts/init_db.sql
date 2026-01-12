-- ============================================================
-- LaCriee - Initialisation Base de Données BigQuery
-- Dataset: lacriee.PROD
-- Region: US
-- ============================================================

-- Vérifier que le dataset existe
-- (À exécuter manuellement si besoin: bq mk --location=US lacriee)

-- ============================================================
-- TABLE 1: ProvidersPrices_Staging
-- Stockage des données brutes extraites des PDF/Excel
-- ============================================================

CREATE TABLE IF NOT EXISTS `lacriee.PROD.ProvidersPrices_Staging` (
  -- Tracking
  job_id STRING NOT NULL OPTIONS(description="Identifiant unique du job d'import"),
  import_timestamp TIMESTAMP NOT NULL OPTIONS(description="Horodatage de l'import"),

  -- Données brutes extraites du PDF/Excel (minimal transformation)
  vendor STRING NOT NULL OPTIONS(description="Fournisseur: laurent_daniel, vvqm, demarne, hennequin"),
  date_extracted DATE NOT NULL OPTIONS(description="Date des prix extraite du document"),
  product_name_raw STRING NOT NULL OPTIONS(description="Nom du produit tel qu'extrait (brut)"),
  code_provider STRING NOT NULL OPTIONS(description="Code fournisseur du produit"),
  price_raw FLOAT64 OPTIONS(description="Prix en EUR/kg"),
  quality_raw STRING OPTIONS(description="Qualité/Calibre brut (ex: Extra, Fraîcheur)"),
  category_raw STRING OPTIONS(description="Catégorie brute extraite du PDF"),

  -- Clé unique
  staging_key STRING NOT NULL OPTIONS(description="Clé unique: {job_id}_{vendor}_{code_provider}_{date}"),

  -- Statut de traitement
  processed BOOLEAN OPTIONS(description="True si transformé vers production"),
  processing_error STRING OPTIONS(description="Message d'erreur si échec de transformation")
)
PARTITION BY DATE(import_timestamp)
CLUSTER BY vendor, date_extracted
OPTIONS(
  description="Table de staging pour données brutes avant transformation SQL",
  labels=[("env", "prod"), ("layer", "staging")]
);

-- ============================================================
-- TABLE 2: ImportJobs
-- Audit trail et tracking de tous les imports
-- ============================================================

CREATE TABLE IF NOT EXISTS `lacriee.PROD.ImportJobs` (
  job_id STRING NOT NULL OPTIONS(description="Identifiant unique du job (UUID)"),

  -- Métadonnées fichier
  filename STRING NOT NULL OPTIONS(description="Nom du fichier uploadé"),
  vendor STRING NOT NULL OPTIONS(description="Fournisseur: laurent_daniel, vvqm, demarne, hennequin"),
  file_size_bytes INT64 OPTIONS(description="Taille du fichier en octets"),
  gcs_url STRING NOT NULL OPTIONS(description="URL du fichier archivé dans GCS"),

  -- Tracking du statut
  status STRING NOT NULL OPTIONS(description="Statut: started, parsing, loading, transforming, completed, failed"),
  status_message STRING OPTIONS(description="Message descriptif du statut actuel"),

  -- Horodatages
  created_at TIMESTAMP NOT NULL OPTIONS(description="Création du job"),
  started_at TIMESTAMP OPTIONS(description="Début du traitement"),
  completed_at TIMESTAMP OPTIONS(description="Fin du traitement"),
  duration_seconds FLOAT64 OPTIONS(description="Durée totale du traitement en secondes"),

  -- Métriques de traitement
  rows_extracted INT64 OPTIONS(description="Nombre de lignes extraites du fichier"),
  rows_loaded_staging INT64 OPTIONS(description="Nombre de lignes chargées dans staging"),
  rows_inserted_prod INT64 OPTIONS(description="Nombre de lignes insérées dans production"),
  rows_updated_prod INT64 OPTIONS(description="Nombre de lignes mises à jour dans production"),
  rows_unknown_products INT64 OPTIONS(description="Nombre de produits non mappés dans CodesNames"),

  -- Gestion des erreurs
  error_message STRING OPTIONS(description="Message d'erreur si échec"),
  error_stacktrace STRING OPTIONS(description="Stack trace complète en cas d'erreur")
)
PARTITION BY DATE(created_at)
CLUSTER BY vendor, status
OPTIONS(
  description="Table d'audit pour tracking complet de tous les imports",
  labels=[("env", "prod"), ("layer", "audit")]
);

-- ============================================================
-- TABLE 3: UnknownProducts
-- Produits présents dans les PDF mais absents de CodesNames
-- ============================================================

CREATE TABLE IF NOT EXISTS `lacriee.PROD.UnknownProducts` (
  -- Identification
  vendor STRING NOT NULL OPTIONS(description="Fournisseur"),
  code_provider STRING NOT NULL OPTIONS(description="Code produit du fournisseur"),
  product_name_raw STRING NOT NULL OPTIONS(description="Nom du produit tel qu'extrait"),

  -- Tracking des occurrences
  first_seen TIMESTAMP NOT NULL OPTIONS(description="Première détection du produit inconnu"),
  last_seen TIMESTAMP NOT NULL OPTIONS(description="Dernière détection du produit inconnu"),
  occurrence_count INT64 OPTIONS(description="Nombre d'occurrences détectées"),

  -- Contexte pour investigation
  job_ids ARRAY<STRING> OPTIONS(description="Liste des job_ids où ce produit a été vu"),
  sample_data JSON OPTIONS(description="Exemple de données (date, prix, catégorie) pour investigation"),

  -- Résolution
  resolved BOOLEAN OPTIONS(description="True si mappé dans CodesNames"),
  resolved_at TIMESTAMP OPTIONS(description="Date de résolution du mapping"),
  mapped_to_code STRING OPTIONS(description="Code CodesNames après résolution"),
  notes STRING OPTIONS(description="Notes de l'utilisateur sur ce produit")
)
PARTITION BY DATE(first_seen)
CLUSTER BY vendor, resolved
OPTIONS(
  description="Produits non mappés nécessitant ajout manuel dans CodesNames",
  labels=[("env", "prod"), ("layer", "reference")]
);

-- ============================================================
-- TABLE 4: ProvidersPrices (Production)
-- Mise à jour pour audit trail
-- ============================================================

-- Vérifier si les colonnes existent déjà avant d'ajouter
-- Note: Exécuter ces ALTER TABLE une seule fois

-- ALTER TABLE `lacriee.PROD.ProvidersPrices`
-- ADD COLUMN IF NOT EXISTS job_id STRING OPTIONS(description="Job d'import ayant créé/mis à jour cette ligne"),
-- ADD COLUMN IF NOT EXISTS import_timestamp TIMESTAMP OPTIONS(description="Horodatage de l'import"),
-- ADD COLUMN IF NOT EXISTS data_source STRING OPTIONS(description="Source: staging ou legacy");

-- ============================================================
-- INDEX / VUES (Optionnel mais recommandé)
-- ============================================================

-- Vue pour dashboard quotidien
CREATE OR REPLACE VIEW `lacriee.PROD.v_daily_import_summary` AS
SELECT
  vendor,
  COUNT(*) AS total_imports,
  COUNTIF(status = 'completed') AS successful,
  COUNTIF(status = 'failed') AS failed,
  COUNTIF(status IN ('started', 'parsing', 'loading', 'transforming')) AS in_progress,
  ROUND(AVG(duration_seconds), 2) AS avg_duration_seconds,
  SUM(rows_inserted_prod) AS total_rows_inserted,
  SUM(rows_updated_prod) AS total_rows_updated,
  SUM(rows_unknown_products) AS total_unknown_products,
  MAX(completed_at) AS last_import_at
FROM `lacriee.PROD.ImportJobs`
WHERE DATE(created_at) = CURRENT_DATE('America/New_York')
GROUP BY vendor
ORDER BY vendor;

-- Vue pour produits à mapper (priorité par fréquence)
CREATE OR REPLACE VIEW `lacriee.PROD.v_products_to_map` AS
SELECT
  vendor,
  code_provider,
  product_name_raw,
  occurrence_count,
  first_seen,
  last_seen,
  ARRAY_LENGTH(job_ids) AS jobs_count,
  sample_data
FROM `lacriee.PROD.UnknownProducts`
WHERE resolved = FALSE
ORDER BY occurrence_count DESC, last_seen DESC;

-- Vue pour monitoring des échecs
CREATE OR REPLACE VIEW `lacriee.PROD.v_failed_jobs` AS
SELECT
  job_id,
  vendor,
  filename,
  created_at,
  error_message,
  gcs_url,
  duration_seconds
FROM `lacriee.PROD.ImportJobs`
WHERE status = 'failed'
  AND DATE(created_at) >= DATE_SUB(CURRENT_DATE('America/New_York'), INTERVAL 7 DAY)
ORDER BY created_at DESC;

-- ============================================================
-- VERIFICATION
-- ============================================================
-- Ces requêtes doivent être exécutées séparément après la création des tables

-- Lister toutes les tables créées
-- SELECT
--   table_name,
--   ROUND(size_bytes / 1024 / 1024, 2) AS size_mb,
--   row_count,
--   creation_time
-- FROM `lacriee.PROD.__TABLES__`
-- ORDER BY table_name;

-- Vérifier que CodesNames existe
-- SELECT COUNT(*) AS codes_count, COUNT(DISTINCT Vendor) AS vendors_count
-- FROM `lacriee.PROD.CodesNames`;
