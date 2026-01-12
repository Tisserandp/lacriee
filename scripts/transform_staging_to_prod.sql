-- ============================================================
-- LaCriee - Transformation ELT: Staging → Production
-- Dataset: lacriee.PROD
-- ============================================================
-- Ce script est exécuté par Python avec @job_id comme paramètre
-- Usage: bq query --parameter=job_id:STRING:abc-123-def ...
-- ============================================================

-- ============================================================
-- ETAPE 1: MERGE vers ProvidersPrices (Production)
-- Jointure avec CodesNames pour normalisation
-- ============================================================

MERGE `lacriee.PROD.ProvidersPrices` AS prod
USING (
  -- Transformer staging data avec mapping CodesNames
  -- Utiliser QUALIFY pour éviter les doublons (garder la dernière ligne par keyDate)
  SELECT DISTINCT
    s.job_id,
    s.import_timestamp,
    s.vendor AS Vendor,
    s.date_extracted AS Date,
    s.code_provider AS Code_Provider,
    s.price_raw AS Prix,

    -- Mapping via CodesNames (fallback sur nom brut si absent)
    COALESCE(cn.Name, s.product_name_raw) AS ProductName,
    COALESCE(cn.Categorie, s.category_raw, 'UNMAPPED') AS Categorie,

    -- Génération de keyDate pour clé unique
    CONCAT(s.code_provider, '_', FORMAT_DATE('%Y-%m-%d', s.date_extracted)) AS keyDate,

    -- Audit trail
    'staging' AS data_source,

    -- Flag pour tracking des produits inconnus
    CASE WHEN cn.Code IS NULL THEN TRUE ELSE FALSE END AS is_unknown_product

  FROM `lacriee.PROD.ProvidersPrices_Staging` s
  LEFT JOIN `lacriee.PROD.CodesNames` cn
    ON s.vendor = cn.Vendor
    AND s.code_provider = cn.Code
  WHERE s.job_id = @job_id  -- Paramètre passé par Python
    AND s.processed = FALSE   -- Seulement les lignes non traitées
  QUALIFY ROW_NUMBER() OVER (PARTITION BY CONCAT(s.code_provider, '_', FORMAT_DATE('%Y-%m-%d', s.date_extracted)) ORDER BY s.import_timestamp DESC) = 1
) AS staging_data

ON prod.keyDate = staging_data.keyDate

WHEN MATCHED THEN
  UPDATE SET
    prod.Vendor = staging_data.Vendor,
    prod.ProductName = staging_data.ProductName,
    prod.Code_Provider = staging_data.Code_Provider,
    prod.Date = staging_data.Date,
    prod.Prix = staging_data.Prix,
    prod.Categorie = staging_data.Categorie,
    prod.job_id = staging_data.job_id,
    prod.import_timestamp = staging_data.import_timestamp,
    prod.data_source = staging_data.data_source

WHEN NOT MATCHED THEN
  INSERT (
    keyDate, Vendor, ProductName, Code_Provider, Date, Prix, Categorie,
    job_id, import_timestamp, data_source
  )
  VALUES (
    staging_data.keyDate,
    staging_data.Vendor,
    staging_data.ProductName,
    staging_data.Code_Provider,
    staging_data.Date,
    staging_data.Prix,
    staging_data.Categorie,
    staging_data.job_id,
    staging_data.import_timestamp,
    staging_data.data_source
  );

-- ============================================================
-- ETAPE 2: Tracker les produits inconnus
-- Insert dans UnknownProducts pour review manuelle
-- ============================================================

MERGE `lacriee.PROD.UnknownProducts` AS target
USING (
  -- Utiliser QUALIFY pour éviter les doublons (garder une seule ligne par vendor+code_provider)
  -- Note: Pas de DISTINCT car JSON ne peut pas être utilisé avec DISTINCT
  SELECT
    s.vendor,
    s.code_provider,
    s.product_name_raw,
    CURRENT_TIMESTAMP() AS detected_at,
    s.job_id,
    JSON_OBJECT(
      'date', CAST(s.date_extracted AS STRING),
      'price', s.price_raw,
      'quality', s.quality_raw,
      'category', s.category_raw
    ) AS sample_data
  FROM `lacriee.PROD.ProvidersPrices_Staging` s
  LEFT JOIN `lacriee.PROD.CodesNames` cn
    ON s.vendor = cn.Vendor AND s.code_provider = cn.Code
  WHERE s.job_id = @job_id
    AND cn.Code IS NULL  -- Produits non mappés uniquement
  QUALIFY ROW_NUMBER() OVER (PARTITION BY s.vendor, s.code_provider ORDER BY s.import_timestamp DESC) = 1
) AS unknown_products

ON target.vendor = unknown_products.vendor
   AND target.code_provider = unknown_products.code_provider
   AND target.resolved = FALSE

WHEN MATCHED THEN
  UPDATE SET
    target.last_seen = unknown_products.detected_at,
    target.occurrence_count = target.occurrence_count + 1,
    target.job_ids = ARRAY_CONCAT(target.job_ids, [unknown_products.job_id]),
    target.sample_data = unknown_products.sample_data  -- Écrase avec dernier exemple

WHEN NOT MATCHED THEN
  INSERT (
    vendor, code_provider, product_name_raw,
    first_seen, last_seen, occurrence_count,
    job_ids, sample_data, resolved
  )
  VALUES (
    unknown_products.vendor,
    unknown_products.code_provider,
    unknown_products.product_name_raw,
    unknown_products.detected_at,
    unknown_products.detected_at,
    1,
    [unknown_products.job_id],
    unknown_products.sample_data,
    FALSE
  );

-- ============================================================
-- ETAPE 3: Marquer staging comme traité
-- ============================================================
-- Note: Cette étape est désactivée car BigQuery ne permet pas UPDATE/MERGE
-- sur des lignes dans le streaming buffer. Les lignes seront marquées
-- comme traitées via un job de nettoyage périodique ou lors d'un prochain run.
-- Pour l'instant, on ignore cette étape pour éviter les erreurs.

-- MERGE désactivé temporairement à cause du streaming buffer
-- MERGE `lacriee.PROD.ProvidersPrices_Staging` AS target
-- USING (
--   SELECT DISTINCT job_id
--   FROM `lacriee.PROD.ProvidersPrices_Staging`
--   WHERE job_id = @job_id
--     AND processed = FALSE
-- ) AS source
-- ON target.job_id = source.job_id
-- WHEN MATCHED THEN
--   UPDATE SET processed = TRUE;

-- ============================================================
-- ETAPE 4: Retourner les statistiques
-- (Pour Python - si exécuté via script séparé)
-- ============================================================

SELECT
  'transformation_stats' AS metric,
  @job_id AS job_id,
  COUNT(*) AS rows_processed,
  COUNTIF(cn.Code IS NOT NULL) AS rows_mapped,
  COUNTIF(cn.Code IS NULL) AS rows_unknown
FROM `lacriee.PROD.ProvidersPrices_Staging` s
LEFT JOIN `lacriee.PROD.CodesNames` cn
  ON s.vendor = cn.Vendor AND s.code_provider = cn.Code
WHERE s.job_id = @job_id;
