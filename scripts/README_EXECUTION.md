# Scripts SQL - Guide d'Exécution

## Prérequis

- `gcloud` CLI installé et authentifié
- `bq` CLI installé (inclus avec gcloud)
- Identifiants GCP avec permissions sur le projet `lacriee`
- Dataset `lacriee.PROD` déjà créé en région `US`

## Phase 1: Initialisation de la Base de Données

### Authentification GCP

```bash
# S'authentifier avec vos identifiants locaux
gcloud auth login

# Vérifier le projet actif
gcloud config get-value project

# Si besoin, définir le projet
gcloud config set project VOTRE_PROJET_ID
```

### Créer le Dataset (si pas déjà fait)

```bash
# Créer le dataset en région US
bq mk --location=US --description="LaCriee Production Dataset" lacriee
```

### Exécuter l'Initialisation

```bash
# Depuis le dossier racine du projet
cd c:\Users\Tisse\OneDrive\Tisserandp\LaCriee

# Exécuter le script d'initialisation
bq query --use_legacy_sql=false < scripts/init_db.sql
```

**Alternative (exécution ligne par ligne):**

```bash
# Si le script complet échoue, exécuter section par section:

# 1. Table Staging
bq query --use_legacy_sql=false '
CREATE TABLE IF NOT EXISTS `lacriee.PROD.ProvidersPrices_Staging` (
  ...
)
'

# 2. Table ImportJobs
bq query --use_legacy_sql=false '
CREATE TABLE IF NOT EXISTS `lacriee.PROD.ImportJobs` (
  ...
)
'

# etc.
```

### Vérification Post-Installation

```bash
# Lister toutes les tables créées
bq ls --format=pretty lacriee.PROD

# Vérifier que CodesNames existe
bq query --use_legacy_sql=false '
SELECT COUNT(*) AS codes_count, COUNT(DISTINCT Vendor) AS vendors_count
FROM `lacriee.PROD.CodesNames`
'

# Tester les vues créées
bq query --use_legacy_sql=false '
SELECT * FROM `lacriee.PROD.v_daily_import_summary`
'
```

## Test du Script de Transformation

### Test avec Job ID Fictif

```bash
# Créer une ligne de test dans staging
bq query --use_legacy_sql=false '
INSERT INTO `lacriee.PROD.ProvidersPrices_Staging`
(job_id, vendor, date_extracted, product_name_raw, code_provider, price_raw, staging_key, processed)
VALUES
("test-job-123", "laurent_daniel", CURRENT_DATE(), "TEST PRODUIT", "LD_TEST_A", 42.50, "test-key-1", FALSE)
'

# Exécuter la transformation avec ce job_id
bq query \
  --use_legacy_sql=false \
  --parameter=job_id:STRING:test-job-123 \
  < scripts/transform_staging_to_prod.sql

# Vérifier le résultat
bq query --use_legacy_sql=false '
SELECT * FROM `lacriee.PROD.ProvidersPrices`
WHERE job_id = "test-job-123"
'

# Nettoyer le test
bq query --use_legacy_sql=false '
DELETE FROM `lacriee.PROD.ProvidersPrices` WHERE job_id = "test-job-123";
DELETE FROM `lacriee.PROD.ProvidersPrices_Staging` WHERE job_id = "test-job-123";
'
```

## Queries Utiles pour Monitoring

### Dashboard Quotidien

```bash
bq query --use_legacy_sql=false '
SELECT * FROM `lacriee.PROD.v_daily_import_summary`
'
```

### Produits à Mapper

```bash
bq query --use_legacy_sql=false '
SELECT
  vendor,
  code_provider,
  product_name_raw,
  occurrence_count,
  first_seen
FROM `lacriee.PROD.v_products_to_map`
LIMIT 20
'
```

### Jobs Échoués

```bash
bq query --use_legacy_sql=false '
SELECT
  job_id,
  vendor,
  filename,
  error_message,
  created_at
FROM `lacriee.PROD.v_failed_jobs`
'
```

## Troubleshooting

### Erreur: "Already Exists"

Si les tables existent déjà:

```bash
# Option 1: Supprimer et recréer (ATTENTION: perte de données!)
bq rm -f -t lacriee.PROD.ProvidersPrices_Staging
bq rm -f -t lacriee.PROD.ImportJobs
bq rm -f -t lacriee.PROD.UnknownProducts

# Puis réexécuter init_db.sql
```

### Erreur: "Permission Denied"

Vérifier les permissions:

```bash
# Lister les datasets accessibles
bq ls

# Tester une query simple
bq query --use_legacy_sql=false 'SELECT 1 AS test'
```

### Erreur: "Dataset not found"

Créer le dataset manuellement:

```bash
bq mk --location=US lacriee
```

## Structure Finale Attendue

Après exécution réussie, vous devriez avoir:

```
lacriee.PROD
├── CodesNames                    (existant - cloné)
├── ProvidersPrices              (existant ou créé)
├── ProvidersPrices_Staging      (nouveau)
├── ImportJobs                   (nouveau)
├── UnknownProducts              (nouveau)
├── v_daily_import_summary       (vue)
├── v_products_to_map            (vue)
└── v_failed_jobs                (vue)
```

## Checklist Phase 1

- [ ] Authentification GCP OK (`gcloud auth login`)
- [ ] Dataset `lacriee.PROD` existe
- [ ] Table `CodesNames` existe et contient des données
- [ ] Script `init_db.sql` exécuté sans erreur
- [ ] 3 nouvelles tables créées (Staging, ImportJobs, UnknownProducts)
- [ ] 3 vues créées (v_daily_import_summary, v_products_to_map, v_failed_jobs)
- [ ] Test de transformation SQL réussi
- [ ] Nettoyage des données de test effectué

## Prochaine Étape

Une fois Phase 1 validée, passer à Phase 2: Implémentation des services Python (`services/bigquery.py`, `services/import_service.py`, etc.)
