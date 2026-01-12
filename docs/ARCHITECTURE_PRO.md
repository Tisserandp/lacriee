# Architecture Professionnelle : LaCriee ELT Pipeline

## Vision

Transformer le parser monolithique en un pipeline ELT resilient avec:
- **Zero timeout** - Reponse HTTP < 1 seconde, traitement async
- **Traçabilite complete** - Chaque import tracke dans BigQuery
- **ELT pur** - Raw data → Staging → Transformation SQL
- **Modularite** - Zero code duplique entre vendors

---

## 1. Resolution du Probleme HTTP Timeout

### Contexte
- n8n envoie POST avec fichiers
- Parsing PDF = 5-30 secondes
- BigQuery MERGE = variable
- Risque timeout HTTP avant fin

### Solution: FastAPI BackgroundTasks

```python
@app.post("/parseLaurentDpdf")
async def parse_laurent_d_pdf(
    file: UploadFile,
    background_tasks: BackgroundTasks,
    x_api_key: str = Header(...)
):
    file_bytes = await file.read()

    # SYNC (< 1 seconde): Archive + create job
    result = service.process_sync(file.filename, file_bytes)

    # ASYNC (background): Parse + load + transform
    background_tasks.add_task(service.process_async, result["job_id"], file_bytes)

    # RETOUR IMMEDIAT à n8n
    return {
        "job_id": result["job_id"],
        "status": "processing",
        "gcs_url": result["gcs_url"],
        "check_status_url": f"/jobs/{result['job_id']}"
    }
```

**Timeline:**
```
0s    : n8n POST /parseLaurentDpdf
0.5s  : API retourne 200 OK avec job_id (connexion fermee)
1-30s : Background task parse + charge dans BigQuery
30s   : Job status = 'completed' dans BigQuery
```

**Avantages:**
- n8n ne timeout jamais
- Pas de Pub/Sub (overkill pour 5 fichiers/jour)
- Processing garanti termine meme si HTTP fermé
- FastAPI gere le lifecycle automatiquement

---

## 2. Architecture ELT (Extract, Load, Transform)

### Configuration Environnement

**Dataset BigQuery:** `lacriee.PROD`
**Region:** `US` (multi-region)
**Table de reference:** `lacriee.PROD.CodesNames` (deja clonee)

### Schema BigQuery

#### Table 1: `ProvidersPrices_Staging` (RAW Data)

```sql
CREATE TABLE `lacriee.PROD.ProvidersPrices_Staging` (
  -- Tracking
  job_id STRING NOT NULL,
  import_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),

  -- Donnees brutes extraites du PDF/Excel
  vendor STRING NOT NULL,
  date_extracted DATE NOT NULL,
  product_name_raw STRING NOT NULL,      -- Ex: "SAUMON ENTIER"
  code_provider STRING NOT NULL,          -- Ex: "LD_SAUMON_E"
  price_raw FLOAT64,
  quality_raw STRING,                     -- Ex: "Extra", "Fraîcheur"
  category_raw STRING,                    -- Categorie brute du PDF

  -- Cle unique
  staging_key STRING NOT NULL,            -- {job_id}_{vendor}_{code_provider}_{date}

  -- Status de traitement
  processed BOOLEAN DEFAULT FALSE,
  processing_error STRING
)
PARTITION BY DATE(import_timestamp)
CLUSTER BY vendor, date_extracted;
```

#### Table 2: `ImportJobs` (Audit Trail)

```sql
CREATE TABLE `lacriee.PROD.ImportJobs` (
  job_id STRING NOT NULL,

  -- Metadata fichier
  filename STRING NOT NULL,
  vendor STRING NOT NULL,
  file_size_bytes INT64,
  gcs_url STRING NOT NULL,

  -- Status tracking
  status STRING NOT NULL,  -- 'started', 'parsing', 'loading', 'transforming', 'completed', 'failed'
  status_message STRING,

  -- Timestamps
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  completed_at TIMESTAMP,
  duration_seconds FLOAT64,

  -- Metriques de traitement
  rows_extracted INT64,
  rows_loaded_staging INT64,
  rows_inserted_prod INT64,
  rows_updated_prod INT64,
  rows_unknown_products INT64,

  -- Erreurs
  error_message STRING,
  error_stacktrace STRING
)
PARTITION BY DATE(created_at)
CLUSTER BY vendor, status;
```

#### Table 3: `UnknownProducts` (Produits non mappes)

```sql
CREATE TABLE `lacriee.PROD.UnknownProducts` (
  -- Identification
  vendor STRING NOT NULL,
  code_provider STRING NOT NULL,
  product_name_raw STRING NOT NULL,

  -- Tracking
  first_seen TIMESTAMP NOT NULL,
  last_seen TIMESTAMP NOT NULL,
  occurrence_count INT64 DEFAULT 1,

  -- Jobs associes
  job_ids ARRAY<STRING>,
  sample_data JSON,  -- Ex: {"date": "2026-01-12", "price": 25.50}

  -- Resolution
  resolved BOOLEAN DEFAULT FALSE,
  resolved_at TIMESTAMP,
  mapped_to_code STRING,
  notes STRING
)
PARTITION BY DATE(first_seen)
CLUSTER BY vendor, resolved;
```

### Flux ELT

```
┌──────────────────────────────────────────────────────────────┐
│  EXTRACT: Parser Python                                      │
│  - Extraction minimale du PDF/Excel                          │
│  - Zero transformation business                              │
│  - Output: Liste de dicts avec donnees brutes               │
└────────────────────────────┬─────────────────────────────────┘
                             │
                             ▼
┌──────────────────────────────────────────────────────────────┐
│  LOAD: Staging Table                                         │
│  - INSERT dans ProvidersPrices_Staging                       │
│  - Donnees RAW, pas de jointure                             │
└────────────────────────────┬─────────────────────────────────┘
                             │
                             ▼
┌──────────────────────────────────────────────────────────────┐
│  TRANSFORM: SQL BigQuery                                     │
│  - JOIN avec CodesNames (mapping)                            │
│  - Normalisation des noms                                    │
│  - Detection produits inconnus                               │
│  - MERGE vers ProvidersPrices (production)                   │
└──────────────────────────────────────────────────────────────┘
```

### SQL de Transformation

Fichier: `sql/transform_staging_to_prod.sql`

```sql
-- Etape 1: MERGE staging -> production avec mapping CodesNames
MERGE `lacriee.PROD.ProvidersPrices` AS prod
USING (
  SELECT
    s.job_id,
    s.import_timestamp,
    s.vendor,
    s.date_extracted AS Date,
    s.code_provider AS Code_Provider,
    s.price_raw AS Prix,

    -- Mapping via CodesNames, fallback sur nom brut
    COALESCE(cn.Name, s.product_name_raw) AS ProductName,
    COALESCE(cn.Categorie, s.category_raw, 'UNMAPPED') AS Categorie,

    -- Cle unique
    CONCAT(s.code_provider, '_', FORMAT_DATE('%Y-%m-%d', s.date_extracted)) AS keyDate,

    -- Audit
    'staging' AS data_source,

    -- Flag unknown products
    CASE WHEN cn.Code IS NULL THEN TRUE ELSE FALSE END AS is_unknown_product

  FROM `lacriee.PROD.ProvidersPrices_Staging` s
  LEFT JOIN `lacriee.PROD.CodesNames` cn
    ON s.vendor = cn.Vendor AND s.code_provider = cn.Code
  WHERE s.job_id = @job_id
    AND s.processed = FALSE
) AS staging_data

ON prod.keyDate = staging_data.keyDate

WHEN MATCHED THEN
  UPDATE SET
    prod.Vendor = staging_data.vendor,
    prod.ProductName = staging_data.ProductName,
    prod.Code_Provider = staging_data.Code_Provider,
    prod.Date = staging_data.Date,
    prod.Prix = staging_data.Prix,
    prod.Categorie = staging_data.Categorie,
    prod.job_id = staging_data.job_id,
    prod.import_timestamp = staging_data.import_timestamp

WHEN NOT MATCHED THEN
  INSERT (keyDate, Vendor, ProductName, Code_Provider, Date, Prix, Categorie, job_id, import_timestamp)
  VALUES (
    staging_data.keyDate, staging_data.vendor, staging_data.ProductName,
    staging_data.Code_Provider, staging_data.Date, staging_data.Prix,
    staging_data.Categorie, staging_data.job_id, staging_data.import_timestamp
  );

-- Etape 2: Tracker les produits inconnus
INSERT INTO `lacriee.PROD.UnknownProducts` (
  vendor, code_provider, product_name_raw,
  first_seen, last_seen, occurrence_count,
  job_ids, sample_data
)
SELECT
  s.vendor,
  s.code_provider,
  s.product_name_raw,
  CURRENT_TIMESTAMP() AS first_seen,
  CURRENT_TIMESTAMP() AS last_seen,
  1 AS occurrence_count,
  [s.job_id] AS job_ids,
  TO_JSON_STRING(STRUCT(
    s.date_extracted AS date,
    s.price_raw AS price,
    s.category_raw AS category
  )) AS sample_data
FROM `lacriee.PROD.ProvidersPrices_Staging` s
LEFT JOIN `lacriee.PROD.CodesNames` cn
  ON s.vendor = cn.Vendor AND s.code_provider = cn.Code
WHERE s.job_id = @job_id
  AND cn.Code IS NULL
  AND NOT EXISTS (
    SELECT 1 FROM `lacriee.PROD.UnknownProducts` up
    WHERE up.vendor = s.vendor AND up.code_provider = s.code_provider AND up.resolved = FALSE
  );

-- Etape 3: Marquer staging comme traite
UPDATE `lacriee.PROD.ProvidersPrices_Staging`
SET processed = TRUE
WHERE job_id = @job_id;
```

### Gestion Produits Inconnus

**Workflow:**

1. Import PDF → Produit "LD_NOUVEAUPRODUIT_A" detecte
2. Pas de match dans CodesNames
3. Insert dans UnknownProducts avec:
   - `vendor`, `code_provider`, `product_name_raw`
   - `occurrence_count = 1`
   - `job_ids = [abc123]`
   - `sample_data = {"date": "2026-01-12", "price": 42.50}`

4. **Query pour review quotidienne:**
```sql
SELECT
  vendor,
  code_provider,
  product_name_raw,
  occurrence_count,
  ARRAY_TO_STRING(job_ids, ', ') AS jobs
FROM `lacriee.PROD.UnknownProducts`
WHERE resolved = FALSE
ORDER BY occurrence_count DESC;
```

5. **Resolution manuelle:**
```sql
-- Ajouter a CodesNames
INSERT INTO `lacriee.PROD.CodesNames` (Vendor, Code, Name, Categorie)
VALUES ('laurent_daniel', 'LD_NOUVEAUPRODUIT_A', 'Nouveau Produit Extra', 'POISSON_BLANC');

-- Marquer resolu
UPDATE `lacriee.PROD.UnknownProducts`
SET resolved = TRUE, resolved_at = CURRENT_TIMESTAMP(), mapped_to_code = 'LD_NOUVEAUPRODUIT_A'
WHERE vendor = 'laurent_daniel' AND code_provider = 'LD_NOUVEAUPRODUIT_A';
```

---

## 3. Service Modulaire: Zero Duplication

### Service d'Import Unifie

Fichier: `services/import_service.py`

```python
"""
Service d'import unifie avec archivage, tracking, et async processing automatiques.
"""
import uuid
import logging
from typing import Callable, Dict, Any, Optional
from datetime import datetime
from fastapi import BackgroundTasks

from services.storage import archive_file
from services.bigquery import (
    create_job_record,
    update_job_status,
    load_raw_to_staging,
    execute_staging_transform
)

logger = logging.getLogger(__name__)


class ImportService:
    """
    Orchestre le pipeline complet:
    1. Archivage GCS
    2. Tracking job
    3. Parsing
    4. Chargement staging
    5. Transformation SQL
    """

    def __init__(self, vendor: str, parser_func: Callable[[bytes], list[dict]]):
        """
        Args:
            vendor: Identifiant fournisseur (laurent_daniel, vvqm, demarne, hennequin)
            parser_func: Fonction de parsing (prend bytes, retourne list[dict])
        """
        self.vendor = vendor
        self.parser_func = parser_func

    def process_sync(self, filename: str, file_bytes: bytes, file_size: int) -> Dict[str, Any]:
        """
        Partie SYNCHRONE (< 1 seconde):
        - Archive fichier GCS
        - Cree job record
        - Retourne job info
        """
        job_id = str(uuid.uuid4())

        try:
            # Archive GCS
            gcs_url = archive_file(self.vendor, filename, file_bytes)
            logger.info(f"[{job_id}] Archived: {gcs_url}")

            # Create job record
            create_job_record(
                job_id=job_id,
                filename=filename,
                vendor=self.vendor,
                file_size_bytes=file_size,
                gcs_url=gcs_url,
                status="started"
            )

            return {
                "job_id": job_id,
                "status": "processing",
                "message": "File received and queued for processing",
                "vendor": self.vendor,
                "filename": filename,
                "gcs_url": gcs_url,
                "check_status_url": f"/jobs/{job_id}"
            }

        except Exception as e:
            logger.exception(f"[{job_id}] Sync error")
            return {"job_id": job_id, "status": "failed", "error": str(e)}

    async def process_async(
        self,
        job_id: str,
        file_bytes: bytes,
        parser_kwargs: Optional[Dict[str, Any]] = None
    ):
        """
        Partie ASYNCHRONE (background):
        - Parse
        - Load staging
        - Transform SQL
        - Update job status
        """
        start_time = datetime.now()

        try:
            # 1. PARSING
            update_job_status(job_id, "parsing", "Extracting data from file")
            parser_kwargs = parser_kwargs or {}
            raw_data = self.parser_func(file_bytes, **parser_kwargs)
            rows_extracted = len(raw_data)
            logger.info(f"[{job_id}] Parsed {rows_extracted} rows")

            # 2. LOAD STAGING
            update_job_status(job_id, "loading", f"Loading {rows_extracted} rows to staging")
            rows_loaded = load_raw_to_staging(job_id, self.vendor, raw_data)
            logger.info(f"[{job_id}] Loaded {rows_loaded} rows to staging")

            # 3. TRANSFORM SQL
            update_job_status(job_id, "transforming", "Running SQL transformations")
            transform_result = execute_staging_transform(job_id)

            rows_inserted = transform_result.get("rows_inserted", 0)
            rows_updated = transform_result.get("rows_updated", 0)
            rows_unknown = transform_result.get("rows_unknown", 0)

            duration = (datetime.now() - start_time).total_seconds()

            # 4. COMPLETE
            update_job_status(
                job_id, "completed", "Import completed successfully",
                rows_extracted=rows_extracted,
                rows_loaded_staging=rows_loaded,
                rows_inserted_prod=rows_inserted,
                rows_updated_prod=rows_updated,
                rows_unknown_products=rows_unknown,
                duration_seconds=duration
            )

            logger.info(
                f"[{job_id}] Completed: {rows_extracted} extracted, "
                f"{rows_inserted} inserted, {rows_updated} updated, {rows_unknown} unknown"
            )

        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            logger.exception(f"[{job_id}] Async error")

            import traceback
            update_job_status(
                job_id, "failed", str(e),
                error_message=str(e),
                error_stacktrace=traceback.format_exc(),
                duration_seconds=duration
            )

    def handle_import(
        self,
        filename: str,
        file_bytes: bytes,
        background_tasks: BackgroundTasks,
        parser_kwargs: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Handler complet:
        - Sync: Archive + job record + retour immediat
        - Async: Parse + load + transform (background)
        """
        file_size = len(file_bytes)

        # Sync (rapide)
        response = self.process_sync(filename, file_bytes, file_size)

        # Queue async
        if response["status"] == "processing":
            background_tasks.add_task(
                self.process_async,
                response["job_id"],
                file_bytes,
                parser_kwargs
            )

        return response
```

### Utilisation dans les Endpoints

Fichier: `main.py`

```python
from fastapi import FastAPI, File, UploadFile, Header, HTTPException, BackgroundTasks
from services.import_service import ImportService
from parsers.laurent_daniel import parse as parse_laurent_daniel
from parsers.vvqm import parse as parse_vvqm
from parsers.demarne import parse as parse_demarne
from parsers.hennequin import parse as parse_hennequin

app = FastAPI()

# Initialiser services (1 ligne par vendor)
ld_service = ImportService("laurent_daniel", parse_laurent_daniel)
vvqm_service = ImportService("vvqm", parse_vvqm)
demarne_service = ImportService("demarne", parse_demarne)
hennequin_service = ImportService("hennequin", parse_hennequin)

def validate_api_key(x_api_key: str):
    if x_api_key != get_api_key():
        raise HTTPException(403, "Invalid API Key")

# Endpoints (4 lignes de code par vendor!)
@app.post("/parseLaurentDpdf")
async def parse_laurent_d_pdf(
    file: UploadFile = File(...),
    background_tasks: BackgroundTasks = BackgroundTasks(),
    x_api_key: str = Header(...)
):
    validate_api_key(x_api_key)
    file_bytes = await file.read()
    return ld_service.handle_import(file.filename, file_bytes, background_tasks)

@app.post("/parseVVQpdf")
async def parse_vvq_pdf(
    file: UploadFile = File(...),
    background_tasks: BackgroundTasks = BackgroundTasks(),
    x_api_key: str = Header(...)
):
    validate_api_key(x_api_key)
    file_bytes = await file.read()
    return vvqm_service.handle_import(file.filename, file_bytes, background_tasks)

@app.post("/parseDemarneXLS")
async def parse_demarne_xls(
    file: UploadFile = File(...),
    background_tasks: BackgroundTasks = BackgroundTasks(),
    date: Optional[str] = Query(None),
    x_api_key: str = Header(...)
):
    validate_api_key(x_api_key)
    file_bytes = await file.read()
    return demarne_service.handle_import(
        file.filename, file_bytes, background_tasks,
        parser_kwargs={"date_fallback": date}
    )

@app.post("/parseHennequinPDF")
async def parse_hennequin_pdf(
    file: UploadFile = File(...),
    background_tasks: BackgroundTasks = BackgroundTasks(),
    x_api_key: str = Header(...)
):
    validate_api_key(x_api_key)
    file_bytes = await file.read()
    return hennequin_service.handle_import(file.filename, file_bytes, background_tasks)

@app.get("/jobs/{job_id}")
async def get_job_status(job_id: str):
    """Endpoint pour n8n polling."""
    from services.bigquery import get_job_status
    job = get_job_status(job_id)
    if not job:
        raise HTTPException(404, "Job not found")
    return job
```

**Benefices:**
- **4 lignes de code** par endpoint (vs 50+ lignes dupliquees)
- Archivage GCS automatique
- Job tracking automatique
- Async processing automatique
- Ajout nouveau vendor = 1 ligne (`new_service = ImportService("nouveau", parse_nouveau)`)

---

## 4. Contrat JSON pour n8n

### Reponse Immediate (POST)

```json
{
  "job_id": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
  "status": "processing",
  "message": "File received and queued for processing",
  "vendor": "laurent_daniel",
  "filename": "CC-84.pdf",
  "gcs_url": "gs://lacriee-archives/laurent_daniel/2026-01-12/f47ac10b_CC-84.pdf",
  "check_status_url": "/jobs/f47ac10b-58cc-4372-a567-0e02b2c3d479"
}
```

### Status Check (GET /jobs/{job_id})

#### Succes:
```json
{
  "job_id": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
  "status": "completed",
  "vendor": "laurent_daniel",
  "filename": "CC-84.pdf",
  "gcs_url": "gs://lacriee-archives/laurent_daniel/2026-01-12/f47ac10b_CC-84.pdf",
  "created_at": "2026-01-12T10:30:00Z",
  "completed_at": "2026-01-12T10:30:15Z",
  "duration_seconds": 15.2,
  "metrics": {
    "rows_extracted": 120,
    "rows_loaded_staging": 120,
    "rows_inserted_prod": 95,
    "rows_updated_prod": 25,
    "rows_unknown_products": 3
  }
}
```

#### Echec:
```json
{
  "job_id": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
  "status": "failed",
  "vendor": "laurent_daniel",
  "filename": "CC-84.pdf",
  "error_message": "Failed to parse PDF: Date not found in document",
  "created_at": "2026-01-12T10:30:00Z",
  "completed_at": "2026-01-12T10:30:05Z"
}
```

#### Succes partiel (produits inconnus):
```json
{
  "job_id": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
  "status": "completed_with_warnings",
  "warnings": ["5 products have unmapped codes"],
  "metrics": {
    "rows_extracted": 50,
    "rows_unknown_products": 5
  }
}
```

### Integration n8n

**Pattern recommande:**

```javascript
// Workflow n8n:
1. HTTP Request: POST /parseLaurentDpdf
   → Store job_id

2. (Optionnel) Wait 10 seconds

3. (Optionnel) HTTP Request: GET /jobs/{job_id}
   → If status = 'completed': Success
   → If status = 'failed': Alert
   → If status = 'processing': Retry apres 5s

// Pattern simple (5 fichiers/jour):
1. POST fichier
2. Recevoir job_id
3. Trust it completes (pas de polling)
```

---

## 5. Queries d'Observabilite

### Dashboard Quotidien

```sql
-- Resume des imports du jour
SELECT
  vendor,
  COUNT(*) AS total_imports,
  COUNTIF(status = 'completed') AS successful,
  COUNTIF(status = 'failed') AS failed,
  ROUND(AVG(duration_seconds), 2) AS avg_duration_sec,
  SUM(rows_inserted_prod) AS total_rows_inserted,
  SUM(rows_unknown_products) AS total_unknown
FROM `lacriee.PROD.ImportJobs`
WHERE DATE(created_at) = CURRENT_DATE()
GROUP BY vendor;
```

### Investigation Echecs

```sql
-- Jobs echoues avec details
SELECT
  job_id,
  vendor,
  filename,
  created_at,
  error_message,
  gcs_url  -- Telecharger fichier pour debug
FROM `lacriee.PROD.ImportJobs`
WHERE status = 'failed'
  AND DATE(created_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
ORDER BY created_at DESC;
```

### Produits a Mapper

```sql
-- Produits inconnus a ajouter dans CodesNames
SELECT
  vendor,
  code_provider,
  product_name_raw,
  occurrence_count,
  first_seen,
  last_seen
FROM `lacriee.PROD.UnknownProducts`
WHERE resolved = FALSE
ORDER BY occurrence_count DESC
LIMIT 50;
```

---

## 6. Roadmap d'Implementation

### Phase 1: Infrastructure BigQuery (Jour 1)
- [ ] Creer table `ProvidersPrices_Staging`
- [ ] Creer table `ImportJobs`
- [ ] Creer table `UnknownProducts`
- [ ] Creer SQL `transform_staging_to_prod.sql`

### Phase 2: Services Core (Jour 2-3)
- [ ] Implementer `services/import_service.py`
- [ ] Implementer `services/bigquery.py` (job tracking + staging load + transform)
- [ ] Implementer `services/storage.py` (archivage GCS)
- [ ] Tests unitaires des services

### Phase 3: Refactor Parsers (Jour 4-5)
- [ ] Refactor parsers pour retourner donnees RAW
- [ ] Deplacer logique categorie vers SQL
- [ ] Tests de non-regression

### Phase 4: API Refactoring (Jour 6)
- [ ] Refactor `main.py` avec `ImportService`
- [ ] Ajouter endpoint `/jobs/{job_id}`
- [ ] Tests integration

### Phase 5: Deployment & Monitoring (Jour 7)
- [ ] Deployer Cloud Run
- [ ] Mettre a jour workflows n8n
- [ ] Dashboard observabilite BigQuery
- [ ] Monitor premiere semaine

---

## Checklist Pre-Implementation

- [ ] Confirmer schema tables BigQuery
- [ ] Confirmer contrat JSON avec n8n
- [ ] Creer bucket GCS `lacriee-archives`
- [ ] Tester BackgroundTasks localement
- [ ] Valider SQL transformation

---

## Comparaison Avant/Apres

| Aspect | Avant | Apres |
|--------|-------|-------|
| **Timeout risk** | Oui (30s parsing) | Non (< 1s response) |
| **Tracabilite** | Logs texte | Table BigQuery |
| **Produits inconnus** | Ignores | Trackes + alerte |
| **Code duplique** | 50+ lignes/endpoint | 4 lignes/endpoint |
| **Transformation** | Python (rigide) | SQL (flexible) |
| **Archivage** | Aucun | GCS automatique |
| **Audit trail** | Impossible | Complet (job_id) |
| **Ajout vendor** | 1h de code | 5 minutes |
