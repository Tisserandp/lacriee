-- ============================================================
-- Création de la table ProvidersPrices (Production)
-- ============================================================

CREATE TABLE IF NOT EXISTS `lacriee.PROD.ProvidersPrices` (
  -- Clé unique
  keyDate STRING NOT NULL OPTIONS(description="Clé unique: {code_provider}_{YYYY-MM-DD}"),
  
  -- Données produit
  Vendor STRING NOT NULL OPTIONS(description="Fournisseur"),
  ProductName STRING NOT NULL OPTIONS(description="Nom du produit (normalisé via CodesNames)"),
  Code_Provider STRING NOT NULL OPTIONS(description="Code fournisseur"),
  Date DATE NOT NULL OPTIONS(description="Date des prix"),
  Prix FLOAT64 OPTIONS(description="Prix en EUR/kg"),
  Categorie STRING OPTIONS(description="Catégorie du produit"),
  
  -- Audit trail (nouveau)
  job_id STRING OPTIONS(description="Job d'import ayant créé/mis à jour cette ligne"),
  import_timestamp TIMESTAMP OPTIONS(description="Horodatage de l'import"),
  data_source STRING OPTIONS(description="Source: staging ou legacy")
)
PARTITION BY Date
CLUSTER BY Vendor, Date
OPTIONS(
  description="Table de production pour les prix des fournisseurs",
  labels=[("env", "prod"), ("layer", "production")]
);
