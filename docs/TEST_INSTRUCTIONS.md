# Instructions de Test - Pipeline ELT

## ✅ Ce qui a été fait

### Phase 1 : Infrastructure BigQuery ✅
- Tables créées : `ProvidersPrices_Staging`, `ImportJobs`, `UnknownProducts`
- Vues créées : `v_daily_import_summary`, `v_products_to_map`, `v_failed_jobs`
- Script SQL de transformation : `scripts/transform_staging_to_prod.sql`

### Phase 2 : Services Core Python ✅
- `services/storage.py` : Archivage GCS
- `services/bigquery.py` : Opérations BigQuery
- `services/import_service.py` : Service orchestrateur
- `config.py` : Configuration centralisée

### Phase 3 : Refactor Parsers ✅
- Structure `parsers/` créée
- Wrappers pour parsers existants

### Phase 4 : API Refactoring ✅
- `main.py` refactorisé avec `ImportService` et `BackgroundTasks`
- Endpoints POST mis à jour
- Endpoint GET `/jobs/{job_id}` ajouté

### Phase 5 : Infrastructure GCS ✅
- Bucket `lacriee-archives` créé
- `requirements.txt` mis à jour avec `google-cloud-storage`

## 🧪 Tests à effectuer

### Option 1 : Test via API (recommandé)

1. **Démarrer le container Docker** :
```bash
docker-compose up -d --build
```

2. **Tester l'endpoint Laurent-Daniel** :
```bash
curl -X POST "http://localhost:8080/parseLaurentDpdf" \
  -H "x-api-key: VOTRE_API_KEY" \
  -F "file=@Samples/LaurentD/CC.pdf"
```

**Réponse attendue** (< 1 seconde) :
```json
{
  "job_id": "uuid-here",
  "status": "processing",
  "message": "File received and queued for processing",
  "vendor": "laurent_daniel",
  "filename": "CC.pdf",
  "gcs_url": "gs://lacriee-archives/laurent_daniel/2026-01-12/CC.pdf",
  "check_status_url": "/jobs/uuid-here"
}
```

3. **Vérifier le statut du job** (attendre 10-30 secondes) :
```bash
curl "http://localhost:8080/jobs/JOB_ID_ICI"
```

**Réponse attendue** :
```json
{
  "job_id": "uuid-here",
  "status": "completed",
  "vendor": "laurent_daniel",
  "filename": "CC.pdf",
  "metrics": {
    "rows_extracted": 120,
    "rows_loaded_staging": 120,
    "rows_inserted_prod": 95,
    "rows_updated_prod": 25,
    "rows_unknown_products": 3
  }
}
```

### Option 2 : Test dans le container Docker

1. **Démarrer le container** :
```bash
docker-compose up -d
```

2. **Exécuter le script de test** :
```bash
docker-compose exec fastapi-pdf-parser python test_import_docker.py
```

### Option 3 : Vérification BigQuery

Vérifier que les données sont bien dans BigQuery :

```bash
# Vérifier les jobs
bq query --use_legacy_sql=false "
SELECT job_id, status, vendor, filename, created_at, duration_seconds
FROM \`lacriee.PROD.ImportJobs\`
ORDER BY created_at DESC
LIMIT 5
"

# Vérifier les données en staging
bq query --use_legacy_sql=false "
SELECT COUNT(*) as count, vendor, DATE(import_timestamp) as import_date
FROM \`lacriee.PROD.ProvidersPrices_Staging\`
GROUP BY vendor, import_date
ORDER BY import_date DESC
"

# Vérifier les produits inconnus
bq query --use_legacy_sql=false "
SELECT vendor, code_provider, product_name_raw, occurrence_count
FROM \`lacriee.PROD.v_products_to_map\`
LIMIT 10
"
```

## 🔍 Points de vérification

1. **Archivage GCS** : Vérifier que le fichier est bien archivé
   ```bash
   gsutil ls gs://lacriee-archives/laurent_daniel/$(date +%Y-%m-%d)/
   ```

2. **Job tracking** : Vérifier que le job apparaît dans `ImportJobs`

3. **Staging** : Vérifier que les données brutes sont dans `ProvidersPrices_Staging`

4. **Production** : Vérifier que les données transformées sont dans `ProvidersPrices`

5. **Produits inconnus** : Vérifier que les produits non mappés apparaissent dans `UnknownProducts`

## ⚠️ Problèmes connus

- **Build Docker** : Problème réseau temporaire lors du téléchargement des packages Debian
  - Solution : Réessayer `docker-compose up -d --build` plus tard
  - Ou : Utiliser un container déjà construit

## 📝 Notes

- Les endpoints retournent maintenant immédiatement (< 1 seconde) avec un `job_id`
- Le traitement se fait en arrière-plan via `BackgroundTasks`
- Le statut peut être vérifié via `/jobs/{job_id}`
- Les données sont archivées automatiquement dans GCS
- Tous les imports sont trackés dans `ImportJobs`

