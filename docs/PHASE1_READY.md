# Phase 1: Infrastructure BigQuery - Prêt à Exécuter

## ✅ Mises à Jour Effectuées

### 1. Configuration Environnement

**Ancien:** `beo-erp.ERPTables.*`
**Nouveau:** `lacriee.PROD.*`

- Dataset: `lacriee.PROD`
- Région: `US` (multi-région)
- Table de référence: `CodesNames` (déjà clonée)

### 2. Fichiers Créés

```
scripts/
├── init_db.sql                   # ✅ Création des 3 tables + vues
├── transform_staging_to_prod.sql # ✅ Logic ELT (staging → prod)
└── README_EXECUTION.md           # ✅ Guide complet d'exécution
```

### 3. Optimisations Appliquées

#### Partitionnement (Cost Optimization)
- `ProvidersPrices_Staging`: **PARTITION BY DATE(import_timestamp)**
- `ImportJobs`: **PARTITION BY DATE(created_at)**
- `UnknownProducts`: **PARTITION BY DATE(first_seen)**

**Bénéfice:** Les requêtes scannent uniquement les partitions nécessaires (jour, semaine, mois) au lieu de tout l'historique.

#### Clustering (Query Performance)
- `ProvidersPrices_Staging`: **CLUSTER BY vendor, date_extracted**
- `ImportJobs`: **CLUSTER BY vendor, status**
- `UnknownProducts`: **CLUSTER BY vendor, resolved**

**Bénéfice:** Données physiquement groupées pour des filtres rapides sur vendor et status.

---

## 📋 Checklist Pré-Exécution

### Vérifications Environnement

- [ ] `gcloud` CLI installé
- [ ] Authentifié avec `gcloud auth login`
- [ ] Projet GCP configuré correctement
- [ ] Dataset `lacriee.PROD` existe
- [ ] Table `CodesNames` existe et contient des données

### Commandes de Vérification

```bash
# 1. Vérifier authentification
gcloud auth list

# 2. Vérifier projet actif
gcloud config get-value project

# 3. Vérifier dataset existe
bq ls lacriee

# 4. Vérifier CodesNames existe
bq query --use_legacy_sql=false '
SELECT COUNT(*) AS row_count, COUNT(DISTINCT Vendor) AS vendors
FROM `lacriee.PROD.CodesNames`
'
```

---

## 🚀 Exécution Phase 1

### Étape 1: Créer les Tables

```bash
cd c:\Users\Tisse\OneDrive\Tisserandp\LaCriee

# Exécuter le script d'initialisation
bq query --use_legacy_sql=false < scripts/init_db.sql
```

**Attendu:** Création de 3 tables + 3 vues

### Étape 2: Vérifier les Tables

```bash
# Lister toutes les tables
bq ls --format=pretty lacriee.PROD

# Devrait afficher:
# - CodesNames (existant)
# - ProvidersPrices (existant ou créé)
# - ProvidersPrices_Staging (nouveau)
# - ImportJobs (nouveau)
# - UnknownProducts (nouveau)
# - v_daily_import_summary (vue)
# - v_products_to_map (vue)
# - v_failed_jobs (vue)
```

### Étape 3: Test de Transformation

```bash
# Insérer une ligne de test
bq query --use_legacy_sql=false '
INSERT INTO `lacriee.PROD.ProvidersPrices_Staging`
(job_id, vendor, date_extracted, product_name_raw, code_provider, price_raw, staging_key, processed)
VALUES
("test-abc-123", "laurent_daniel", CURRENT_DATE(), "SAUMON TEST", "LD_SAUMON_TEST", 35.00, "test-key-1", FALSE)
'

# Exécuter la transformation
bq query \
  --use_legacy_sql=false \
  --parameter=job_id:STRING:test-abc-123 \
  < scripts/transform_staging_to_prod.sql

# Vérifier le résultat
bq query --use_legacy_sql=false '
SELECT * FROM `lacriee.PROD.ProvidersPrices`
WHERE job_id = "test-abc-123"
'

# ✅ Attendu: 1 ligne insérée avec ProductName normalisé via CodesNames

# Nettoyer le test
bq query --use_legacy_sql=false '
DELETE FROM `lacriee.PROD.ProvidersPrices` WHERE job_id = "test-abc-123";
DELETE FROM `lacriee.PROD.ProvidersPrices_Staging` WHERE job_id = "test-abc-123";
'
```

---

## 📊 Schémas des Tables

### Table: ProvidersPrices_Staging

| Colonne | Type | Description |
|---------|------|-------------|
| job_id | STRING | UUID du job d'import |
| import_timestamp | TIMESTAMP | Horodatage insertion (pour partition) |
| vendor | STRING | laurent_daniel, vvqm, demarne, hennequin |
| date_extracted | DATE | Date des prix extraite du PDF |
| product_name_raw | STRING | Nom brut du produit |
| code_provider | STRING | Code fournisseur (ex: LD_SAUMON_E) |
| price_raw | FLOAT64 | Prix en EUR/kg |
| quality_raw | STRING | Qualité/calibre |
| category_raw | STRING | Catégorie brute PDF |
| staging_key | STRING | Clé unique |
| processed | BOOLEAN | True si transformé → prod |
| processing_error | STRING | Message d'erreur éventuel |

**Partitionnement:** `DATE(import_timestamp)`
**Clustering:** `vendor, date_extracted`

### Table: ImportJobs

| Colonne | Type | Description |
|---------|------|-------------|
| job_id | STRING | UUID du job |
| filename | STRING | Nom du fichier uploadé |
| vendor | STRING | Fournisseur |
| file_size_bytes | INT64 | Taille du fichier |
| gcs_url | STRING | URL GCS du fichier archivé |
| status | STRING | started, parsing, loading, transforming, completed, failed |
| status_message | STRING | Message descriptif |
| created_at | TIMESTAMP | Création du job |
| completed_at | TIMESTAMP | Fin du job |
| duration_seconds | FLOAT64 | Durée totale |
| rows_extracted | INT64 | Lignes extraites |
| rows_loaded_staging | INT64 | Lignes en staging |
| rows_inserted_prod | INT64 | Lignes insérées prod |
| rows_updated_prod | INT64 | Lignes mises à jour prod |
| rows_unknown_products | INT64 | Produits non mappés |
| error_message | STRING | Message d'erreur |
| error_stacktrace | STRING | Stack trace complète |

**Partitionnement:** `DATE(created_at)`
**Clustering:** `vendor, status`

### Table: UnknownProducts

| Colonne | Type | Description |
|---------|------|-------------|
| vendor | STRING | Fournisseur |
| code_provider | STRING | Code produit |
| product_name_raw | STRING | Nom brut |
| first_seen | TIMESTAMP | Première détection |
| last_seen | TIMESTAMP | Dernière détection |
| occurrence_count | INT64 | Nombre d'occurrences |
| job_ids | ARRAY<STRING> | Jobs où détecté |
| sample_data | JSON | Exemple de données |
| resolved | BOOLEAN | True si mappé |
| resolved_at | TIMESTAMP | Date de résolution |
| mapped_to_code | STRING | Code CodesNames |
| notes | STRING | Notes utilisateur |

**Partitionnement:** `DATE(first_seen)`
**Clustering:** `vendor, resolved`

---

## 🔍 Vues Créées

### v_daily_import_summary
Dashboard quotidien avec métriques par vendor:
- Total imports
- Successful/Failed counts
- Avg duration
- Total rows inserted/updated
- Unknown products count

### v_products_to_map
Liste des produits non mappés triés par:
- Occurrence count (DESC)
- Last seen (DESC)

### v_failed_jobs
Jobs échoués des 7 derniers jours avec:
- Error message
- GCS URL pour investigation
- Duration

---

## ⚠️ Points de Vigilance

### 1. Projet ID

Les scripts utilisent `lacriee.PROD` (dataset seulement).

Si votre projet GCP a un ID différent de "lacriee", les scripts fonctionneront quand même car BigQuery utilise le projet actif par défaut (`gcloud config get-value project`).

**Pour expliciter le projet:**
```sql
-- Remplacer
`lacriee.PROD.TableName`
-- Par
`VOTRE_PROJET_ID.lacriee.PROD.TableName`
```

### 2. Région US

Le dataset est en multi-région **US**. Si vous avez des contraintes RGPD ou latence Europe, contactez-moi pour adapter la région.

### 3. CodesNames

Le script suppose que `lacriee.PROD.CodesNames` existe déjà. Si absent:

```bash
# Vérifier
bq show lacriee.PROD.CodesNames

# Si erreur "Not found", la table doit être créée ou clonée
```

---

## ✅ Validation Phase 1

Une fois l'exécution terminée, vérifier:

1. **Tables créées:** `bq ls lacriee.PROD` montre 3+ tables
2. **Vues créées:** Les 3 vues apparaissent dans la liste
3. **Test transformation:** Le test INSERT/TRANSFORM/DELETE fonctionne
4. **Aucune erreur:** Logs propres sans erreurs BigQuery

**Si tout est OK:** Phase 1 ✅ → Passer à Phase 2 (Services Python)

---

## 📞 Support

Si erreurs lors de l'exécution:

1. Copier le message d'erreur complet
2. Copier la commande qui a échoué
3. Vérifier les prérequis (auth, dataset, permissions)
4. Consulter `scripts/README_EXECUTION.md` section Troubleshooting
