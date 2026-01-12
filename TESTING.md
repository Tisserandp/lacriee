# 🧪 Procédure de Test - LaCriee Pipeline ELT

**Version:** 1.0
**Date:** 2026-01-12
**Statut:** Production Ready

---

## 📋 Table des Matières

1. [Vue d'Ensemble](#vue-densemble)
2. [Prérequis](#prérequis)
3. [Structure des Tests](#structure-des-tests)
4. [Tests Unitaires](#tests-unitaires)
5. [Tests d'Intégration](#tests-dintégration)
6. [Tests End-to-End](#tests-end-to-end)
7. [Tests de Régression](#tests-de-régression)
8. [Validation BigQuery](#validation-bigquery)
9. [Checklist de Test](#checklist-de-test)
10. [Troubleshooting](#troubleshooting)

---

## 🎯 Vue d'Ensemble

Le pipeline ELT LaCriee comporte **3 niveaux de tests**:

```
┌─────────────────────────────────────────────────────────┐
│  1. TESTS UNITAIRES                                     │
│     → Parsers individuels (PDF/Excel parsing)           │
│     → Services (GCS, BigQuery)                          │
│     → Utilitaires (sanitize, validation)                │
└─────────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────────┐
│  2. TESTS D'INTÉGRATION                                 │
│     → ImportService (sync + async)                      │
│     → Pipeline complet sans API HTTP                    │
└─────────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────────┐
│  3. TESTS END-TO-END                                    │
│     → API HTTP complète                                 │
│     → Fichiers samples réels                            │
│     → Validation BigQuery                               │
└─────────────────────────────────────────────────────────┘
```

---

## ✅ Prérequis

### 1. Environnement Docker
```bash
# Vérifier que le conteneur est actif
docker ps | grep fastapi-pdf-parser

# Si nécessaire, démarrer le conteneur
docker-compose up -d
```

### 2. Fichiers Sample
Les fichiers de test doivent être présents dans `Samples/`:

```
Samples/
├── LaurentD/
│   └── CC.pdf (149KB)
├── VVQ/
│   └── GEXPORT.pdf (162KB)
├── Demarne/
│   └── Classeur1 G19.xlsx (2MB)
└── Hennequin/
    └── (fichier PDF à ajouter)
```

### 3. Credentials BigQuery
```bash
# Vérifier que les credentials sont montés dans Docker
docker exec fastapi-pdf-parser ls -la /app/credentials.json
```

### 4. Dataset BigQuery
Le dataset `lacriee.PROD` doit exister avec toutes les tables:
- `ProvidersPrices_Staging`
- `ImportJobs`
- `UnknownProducts`
- `ProvidersPrices`
- `CodesNames`

---

## 📁 Structure des Tests

```
tests/
├── __init__.py                    # Module de tests
├── test_all_samples.py            # ⭐ Tests end-to-end complets
├── test_direct.py                 # Tests d'intégration (sans HTTP)
├── test_vvqm_debug.py             # Debug parser VVQM
├── test_demarne_debug.py          # Debug parser Demarne
└── unit/                          # (À créer) Tests unitaires
    ├── test_parsers.py
    ├── test_services.py
    └── test_utils.py
```

---

## 🔬 Tests Unitaires

### Objectif
Tester chaque composant isolément sans dépendances externes.

### Tests à Créer (Optionnel - Pytest)

```python
# tests/unit/test_parsers.py
import pytest
from main import extract_LD_data_from_pdf, parse_vvq_pdf_data, parse_demarne_excel_data

def test_laurent_daniel_parser():
    """Test du parser Laurent-Daniel avec un PDF valide."""
    with open("Samples/LaurentD/CC.pdf", "rb") as f:
        data = extract_LD_data_from_pdf(f.read())

    assert len(data) == 96
    assert "Code_Provider" in data[0]
    assert "Prix" in data[0]
    assert "ProductName" in data[0]

def test_vvqm_parser():
    """Test du parser VVQM avec un PDF valide."""
    with open("Samples/VVQ/GEXPORT.pdf", "rb") as f:
        data = parse_vvq_pdf_data(f.read())

    assert len(data) == 89
    assert data[0]["Vendor"] == "vvqm"

def test_demarne_parser():
    """Test du parser Demarne avec un Excel valide."""
    with open("Samples/Demarne/Classeur1 G19.xlsx", "rb") as f:
        data = parse_demarne_excel_data(f.read())

    assert len(data) == 691
    assert data[0]["Vendor"] == "demarne"
```

### Exécution
```bash
# Dans le conteneur Docker
docker exec fastapi-pdf-parser pytest tests/unit/ -v
```

---

## 🔗 Tests d'Intégration

### Objectif
Tester le pipeline complet sans passer par l'API HTTP.

### Test Principal: `test_direct.py`

Ce test valide:
1. ✅ Archivage GCS
2. ✅ Création du job dans ImportJobs
3. ✅ Parsing du fichier
4. ✅ Chargement en staging
5. ✅ Transformation SQL
6. ✅ Insertion en production

### Exécution
```bash
# Test Laurent-Daniel uniquement
docker exec fastapi-pdf-parser python tests/test_direct.py

# Résultat attendu:
# ✅ Pipeline terminé avec succès!
# Exit code: 0
```

### Critères de Succès
- ✅ Exit code = 0
- ✅ Statut job = "completed" (ou "started" si streaming buffer)
- ✅ Rows extracted > 0
- ✅ Rows inserted prod > 0
- ✅ GCS URL valide

---

## 🌐 Tests End-to-End

### Test Principal: `test_all_samples.py`

Ce test valide **tous les vendors** avec leurs fichiers samples réels.

### Vendors Testés
1. **Laurent-Daniel** (CC.pdf) → 96 lignes
2. **VVQM** (GEXPORT.pdf) → 89 lignes
3. **Demarne** (Classeur1 G19.xlsx) → 691 lignes
4. **Hennequin** (⏳ en attente de sample)

### Exécution
```bash
# Test complet de tous les vendors
docker exec fastapi-pdf-parser python tests/test_all_samples.py

# Durée attendue: ~5-6 minutes (incluant attentes streaming buffer)
```

### Résultat Attendu
```
============================================================
=== RÉSUMÉ DES TESTS ===
============================================================
laurent_daniel: ✅ Succès
vvqm: ✅ Succès
demarne: ✅ Succès

Total: 3/3 tests réussis
```

### Critères de Succès
- ✅ 3/3 tests réussis (ou 4/4 avec Hennequin)
- ✅ Tous les jobs créés dans ImportJobs
- ✅ Toutes les données en production (ProvidersPrices)
- ✅ Unknown products détectés (UnknownProducts)
- ✅ Pas d'exceptions Python

---

## 🐛 Tests de Régression

### Objectif
Détecter les bugs spécifiques qui ont été corrigés.

### Bugs Historiques à Re-Tester

#### 1. Parser Laurent-Daniel - Valeurs Non-Numériques
**Bug:** Crash sur valeurs "Pelee" (non-numériques)
**Fix:** `pd.to_numeric(errors='coerce')`

**Test:**
```bash
docker exec fastapi-pdf-parser python tests/test_direct.py
# Doit passer sans erreur
```

#### 2. VVQM - Date Extraction
**Bug:** Date regex ne matchait pas le format PDF
**Fix:** Regex étendu dans `parse_vvq_pdf_data()`

**Test:**
```bash
docker exec fastapi-pdf-parser python tests/test_vvqm_debug.py
# Résultat attendu: 89 lignes parsées
```

#### 3. Demarne - Excel Header
**Bug:** Erreur sur lecture de l'en-tête Excel
**Fix:** Gestion robuste dans `extract_date_from_excel_header()`

**Test:**
```bash
docker exec fastapi-pdf-parser python tests/test_demarne_debug.py
# Résultat attendu: 691 lignes parsées
```

#### 4. BigQuery - MERGE Duplicates
**Bug:** "UPDATE/MERGE must match at most one source row"
**Fix:** QUALIFY ROW_NUMBER() dans `transform_staging_to_prod.sql`

**Validation:**
```sql
-- Aucun doublon dans la requête MERGE
SELECT keyDate, COUNT(*) AS cnt
FROM (
  SELECT CONCAT(code_provider, '_', FORMAT_DATE('%Y-%m-%d', date_extracted)) AS keyDate
  FROM `lacriee.PROD.ProvidersPrices_Staging`
  WHERE job_id = 'JOB_ID_HERE'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY CONCAT(code_provider, '_', FORMAT_DATE('%Y-%m-%d', date_extracted)) ORDER BY import_timestamp DESC) = 1
)
GROUP BY keyDate
HAVING cnt > 1;
-- Résultat attendu: 0 lignes
```

---

## 📊 Validation BigQuery

### Après Chaque Test, Valider:

#### 1. Jobs Créés
```sql
SELECT
  job_id,
  vendor,
  filename,
  status,
  rows_extracted,
  rows_inserted_prod,
  rows_unknown_products,
  created_at
FROM `lacriee.PROD.ImportJobs`
ORDER BY created_at DESC
LIMIT 10;
```

**Critères:**
- ✅ Job présent dans la table
- ✅ `rows_extracted` > 0
- ✅ `rows_inserted_prod` > 0 (ou proche de `rows_extracted`)

#### 2. Données en Production
```sql
SELECT
  vendor,
  COUNT(*) AS total_rows,
  COUNT(DISTINCT Code_Provider) AS unique_codes,
  MIN(Date) AS first_date,
  MAX(Date) AS last_date
FROM `lacriee.PROD.ProvidersPrices`
WHERE job_id IN (
  SELECT job_id
  FROM `lacriee.PROD.ImportJobs`
  WHERE DATE(created_at) = CURRENT_DATE()
)
GROUP BY vendor
ORDER BY vendor;
```

**Critères:**
- ✅ Nombre de lignes cohérent avec parsing
- ✅ Dates valides
- ✅ Codes fournisseurs non NULL

#### 3. Unknown Products
```sql
SELECT
  vendor,
  code,
  raw_name,
  occurrence_count,
  last_seen_at
FROM `lacriee.PROD.UnknownProducts`
WHERE last_seen_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
ORDER BY occurrence_count DESC
LIMIT 20;
```

**Critères:**
- ✅ Produits non mappés détectés (normal si CodesNames vide)
- ✅ `occurrence_count` > 0

#### 4. Staging Nettoyage (Optionnel)
```sql
-- Vérifier que les lignes sont marquées comme traitées
SELECT
  job_id,
  COUNT(*) AS total,
  SUM(CASE WHEN processed = TRUE THEN 1 ELSE 0 END) AS processed_count
FROM `lacriee.PROD.ProvidersPrices_Staging`
WHERE DATE(import_timestamp) = CURRENT_DATE()
GROUP BY job_id;
```

**Note:** Le flag `processed` peut rester à FALSE à cause du streaming buffer (limitation connue).

---

## ✅ Checklist de Test Complète

### Avant Chaque Release

- [ ] **1. Tests Unitaires (si implémentés)**
  ```bash
  docker exec fastapi-pdf-parser pytest tests/unit/ -v
  ```

- [ ] **2. Tests d'Intégration**
  ```bash
  docker exec fastapi-pdf-parser python tests/test_direct.py
  ```
  → Résultat: Exit code 0

- [ ] **3. Tests End-to-End**
  ```bash
  docker exec fastapi-pdf-parser python tests/test_all_samples.py
  ```
  → Résultat: 3/3 (ou 4/4) tests réussis

- [ ] **4. Tests de Régression**
  - [ ] Test VVQM debug → 89 lignes
  - [ ] Test Demarne debug → 691 lignes
  - [ ] Test Laurent-Daniel debug → 96 lignes

- [ ] **5. Validation BigQuery**
  - [ ] Jobs créés et visibles dans ImportJobs
  - [ ] Données en production (ProvidersPrices)
  - [ ] Unknown products détectés (UnknownProducts)
  - [ ] Pas de doublons dans MERGE

- [ ] **6. Tests API HTTP (Manuel)**
  ```bash
  # Test via curl
  curl -X POST "http://localhost:8080/parseLaurentDpdf" \
    -H "x-api-key: YOUR_API_KEY" \
    -F "file=@Samples/LaurentD/CC.pdf"

  # Vérifier le statut
  curl "http://localhost:8080/jobs/JOB_ID"
  ```

- [ ] **7. Tests de Performance**
  - [ ] Petit fichier (< 200KB) → < 2 minutes
  - [ ] Gros fichier (2MB Excel) → < 3 minutes
  - [ ] 5 fichiers en parallèle → pas de crash

- [ ] **8. Tests de Charge (Optionnel)**
  ```bash
  # Simuler 10 imports simultanés
  for i in {1..10}; do
    curl -X POST "http://localhost:8080/parseLaurentDpdf" \
      -H "x-api-key: YOUR_API_KEY" \
      -F "file=@Samples/LaurentD/CC.pdf" &
  done
  wait
  ```

---

## 🐛 Troubleshooting

### Test Échoue: "File not found"
**Cause:** Fichiers samples manquants
**Solution:**
```bash
# Vérifier les fichiers
ls -lh Samples/LaurentD/
ls -lh Samples/VVQ/
ls -lh Samples/Demarne/
```

### Test Échoue: "No module named 'main'"
**Cause:** Mauvais répertoire de travail
**Solution:**
```bash
# Exécuter depuis la racine du projet
cd /app && python tests/test_all_samples.py
```

### Test Échoue: "Streaming buffer rows cannot be modified"
**Cause:** BigQuery streaming buffer actif (normal)
**Impact:** Le statut du job peut rester à "started"
**Validation:**
```sql
-- Vérifier manuellement les données
SELECT COUNT(*) FROM `lacriee.PROD.ProvidersPrices`
WHERE job_id = 'JOB_ID_HERE';
```
**Solution:** Attendre 1-2 minutes et vérifier à nouveau

### Test Échoue: "Job not found in BigQuery"
**Cause:** Credentials BigQuery incorrectes
**Solution:**
```bash
# Vérifier les credentials
docker exec fastapi-pdf-parser python -c "
from services.bigquery import get_bigquery_client
client = get_bigquery_client()
print(f'Project: {client.project}')
"
```

### Test Échoue: "Parser error"
**Cause:** Format PDF/Excel inattendu
**Solution:**
```bash
# Débugger le parser spécifique
docker exec fastapi-pdf-parser python tests/test_vvqm_debug.py
docker exec fastapi-pdf-parser python tests/test_demarne_debug.py
```

### Test Passe Mais Pas de Données en Production
**Cause:** Transformation SQL échouée silencieusement
**Solution:**
```bash
# Vérifier les logs
docker logs fastapi-pdf-parser | grep "ERROR"

# Vérifier le staging
bq query --use_legacy_sql=false '
SELECT * FROM `lacriee.PROD.ProvidersPrices_Staging`
WHERE job_id = "JOB_ID_HERE"
LIMIT 10
'
```

---

## 📝 Rapport de Test

Après chaque session de tests, remplir ce rapport:

```markdown
## Test Report - [DATE]

### Environment
- Docker Image: lacriee-fastapi-pdf-parser
- Python Version: 3.10
- BigQuery Dataset: lacriee.PROD
- GCS Bucket: lacriee-archives

### Tests Executed
- [ ] Tests Unitaires
- [x] Tests d'Intégration
- [x] Tests End-to-End
- [x] Tests de Régression
- [x] Validation BigQuery

### Results
| Test | Status | Rows | Duration | Notes |
|------|--------|------|----------|-------|
| Laurent-Daniel | ✅ | 96 | 1m35s | OK |
| VVQM | ✅ | 89 | 1m38s | OK |
| Demarne | ✅ | 691→679 | 1m17s | 12 doublons filtrés (normal) |
| Hennequin | ⏳ | - | - | En attente sample |

### Issues Found
- Aucun

### BigQuery Validation
- ✅ Jobs créés: 3/3
- ✅ Production data: 864 rows
- ✅ Unknown products: 876 detected
- ✅ No duplicates in MERGE

### Recommendation
✅ READY FOR PRODUCTION
```

---

## 🚀 Commandes de Test Rapides

```bash
# Test complet (recommandé avant chaque release)
docker exec fastapi-pdf-parser python tests/test_all_samples.py

# Test rapide d'intégration
docker exec fastapi-pdf-parser python tests/test_direct.py

# Debug parsers individuels
docker exec fastapi-pdf-parser python tests/test_vvqm_debug.py
docker exec fastapi-pdf-parser python tests/test_demarne_debug.py

# Vérifier BigQuery après tests
bq query --use_legacy_sql=false '
SELECT vendor, COUNT(*) AS total
FROM `lacriee.PROD.ProvidersPrices`
WHERE DATE(import_timestamp) = CURRENT_DATE()
GROUP BY vendor
'
```

---

## 📚 Références

- [Architecture Détaillée](docs/ARCHITECTURE_PRO.md)
- [Résultats Tests](docs/TESTS_RESULTS.md)
- [Documentation Finale](docs/PROJET_FINAL.md)
- [Phase 1 Setup](docs/PHASE1_READY.md)

---

**Dernière Mise à Jour:** 2026-01-12
**Version:** 1.0
**Auteur:** Pipeline LaCriee Team
