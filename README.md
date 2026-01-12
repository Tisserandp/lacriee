# 🐟 LaCriee - Seafood Price Parser & ELT Pipeline

**Version:** 1.0.0
**Status:** ✅ Production Ready
**Last Updated:** 2026-01-12

---

## 📋 Overview

LaCriee is a FastAPI-based ELT (Extract, Load, Transform) pipeline that automatically imports seafood price lists from PDF and Excel files into BigQuery for analysis and integration with the BEO ERP system.

### Key Features

- ✅ **Multi-Vendor Support:** Laurent-Daniel (PDF), VVQM (PDF), Demarne (Excel), Hennequin (PDF)
- ✅ **Async Processing:** FastAPI BackgroundTasks for instant response (<1s)
- ✅ **GCS Archival:** Automatic file archiving to Google Cloud Storage
- ✅ **BigQuery Pipeline:** Staging → Transform → Production with SQL
- ✅ **Job Tracking:** Complete audit trail in ImportJobs table
- ✅ **Unknown Product Detection:** Automatically detects unmapped products

### Pipeline Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│  1. HTTP POST /parseVendorPdf                                    │
│     → FastAPI receives file (sync, <1s)                          │
└──────────────────────────────────────────────────────────────────┘
                            ↓
┌──────────────────────────────────────────────────────────────────┐
│  2. Archive to GCS                                               │
│     → gs://lacriee-archives/{vendor}/{YYYY-MM-DD}/file           │
│     → Create job record in BigQuery ImportJobs                   │
└──────────────────────────────────────────────────────────────────┘
                            ↓
┌──────────────────────────────────────────────────────────────────┐
│  3. Background Processing (async)                                │
│     → Parse PDF/Excel                                            │
│     → Load to ProvidersPrices_Staging                            │
│     → Execute SQL transformation                                 │
│     → Merge to ProvidersPrices (production)                      │
│     → Detect unknown products → UnknownProducts table            │
└──────────────────────────────────────────────────────────────────┘
```

---

## 🚀 Quick Start

### 1. Start the Docker Container

```bash
docker-compose up -d
```

### 2. Verify the Service

```bash
# Check container is running
docker ps | grep fastapi-pdf-parser

# Check API health
curl http://localhost:8080/health
```

### 3. Import a File

```bash
curl -X POST "http://localhost:8080/parseLaurentDpdf" \
  -H "x-api-key: YOUR_API_KEY" \
  -F "file=@Samples/LaurentD/CC.pdf"
```

### 4. Check Job Status

```bash
curl "http://localhost:8080/jobs/{job_id}"
```

---

## 📁 Project Structure

```
lacriee/
├── main.py                         # FastAPI app + parsers (1142 lines)
├── config.py                       # Configuration centralisée
├── requirements.txt                # Python dependencies
├── Dockerfile                      # Docker image
├── docker-compose.yml              # Docker orchestration
│
├── services/                       # Core services
│   ├── __init__.py
│   ├── storage.py                  # GCS archival (82 lines)
│   ├── bigquery.py                 # BigQuery operations (450+ lines)
│   └── import_service.py           # ELT orchestrator (165 lines)
│
├── parsers/                        # Parser wrappers (optional)
│   ├── __init__.py
│   ├── laurent_daniel.py
│   └── utils.py
│
├── scripts/                        # SQL scripts
│   ├── init_db.sql                 # Create all BigQuery tables
│   └── transform_staging_to_prod.sql  # SQL transformation logic
│
├── tests/                          # Test suite
│   ├── __init__.py
│   ├── test_all_samples.py         # ⭐ End-to-end tests (all vendors)
│   ├── test_direct.py              # Integration tests
│   ├── test_vvqm_debug.py          # VVQM parser debug
│   └── test_demarne_debug.py       # Demarne parser debug
│
├── Samples/                        # Test files
│   ├── LaurentD/CC.pdf
│   ├── VVQ/GEXPORT.pdf
│   └── Demarne/Classeur1 G19.xlsx
│
├── docs/                           # Documentation
│   ├── ARCHITECTURE_PRO.md         # Detailed architecture
│   ├── PROJET_FINAL.md             # Final project documentation
│   ├── TESTS_RESULTS.md            # Test results and bugs fixed
│   ├── PHASE1_READY.md             # Phase 1 guide
│   └── REFACTORING_PLAN.md         # Original refactoring plan
│
├── TESTING.md                      # ⭐ Testing procedure (THIS IS KEY)
└── README.md                       # This file
```

---

## 🧪 Testing

### Run Complete Test Suite

```bash
# End-to-end tests for all vendors (recommended)
docker exec fastapi-pdf-parser python tests/test_all_samples.py

# Expected output:
# laurent_daniel: ✅ Succès
# vvqm: ✅ Succès
# demarne: ✅ Succès
# Total: 3/3 tests réussis
```

### Run Individual Tests

```bash
# Integration test (Laurent-Daniel only)
docker exec fastapi-pdf-parser python tests/test_direct.py

# Debug specific parsers
docker exec fastapi-pdf-parser python tests/test_vvqm_debug.py
docker exec fastapi-pdf-parser python tests/test_demarne_debug.py
```

### Full Testing Documentation

See **[TESTING.md](TESTING.md)** for comprehensive testing procedures, validation queries, and troubleshooting.

---

## 📊 BigQuery Schema

### Tables Created

| Table | Description | Rows (example) |
|-------|-------------|----------------|
| `ProvidersPrices_Staging` | Raw parsed data | 876 |
| `ImportJobs` | Job audit trail | 3 |
| `UnknownProducts` | Unmapped products | 876 |
| `ProvidersPrices` | **Production data** | 864 |
| `CodesNames` | Product mapping | (to be populated) |

### Views Created

- `v_daily_import_summary` - Daily import dashboard
- `v_products_to_map` - Products needing mapping (by frequency)
- `v_failed_jobs` - Failed jobs (last 7 days)

---

## 🔧 Configuration

### Environment Variables

```bash
# .env (local development)
GCP_PROJECT_ID=beo-erp
BQ_DATASET=lacriee.PROD
BQ_LOCATION=US
GCS_BUCKET=lacriee-archives
```

### Docker Environment

```yaml
# docker-compose.yml
environment:
  - GOOGLE_APPLICATION_CREDENTIALS=/app/credentials.json
  - BQ_DATASET=lacriee.PROD
  - GCS_BUCKET=lacriee-archives
```

---

## 📈 Current Status

### Test Results (2026-01-12)

| Vendor | File | Parsed | Production | Status |
|--------|------|--------|------------|--------|
| Laurent-Daniel | CC.pdf | 96 | 96 | ✅ |
| VVQM | GEXPORT.pdf | 89 | 89 | ✅ |
| Demarne | Classeur1 G19.xlsx | 691 | 679 | ✅ |
| Hennequin | *(pending)* | - | - | ⏳ |

**Total:** 876 lines parsed → 864 lines in production

### Known Limitations

1. **Streaming Buffer Delay:** Job status updates may be delayed 1-2 minutes due to BigQuery streaming buffer. Data is correctly inserted even if status shows "started".

2. **Unknown Products:** 100% of products marked as "unknown" because `CodesNames` table needs manual mapping.

3. **Hennequin:** Not yet tested (waiting for sample file).

---

## 🛠️ Development

### Add a New Vendor

1. Create parser function in `main.py`:
```python
def parse_new_vendor_pdf(file_bytes: bytes) -> list[dict]:
    # Parse logic here
    return [
        {
            "Code_Provider": "CODE123",
            "ProductName": "Product Name",
            "Prix": 12.50,
            "Date": "2026-01-12",
            "Categorie": "CATEGORY",
            "Vendor": "new_vendor",
            "keyDate": "CODE123_2026-01-12"
        }
    ]
```

2. Add endpoint in `main.py`:
```python
@app.post("/parseNewVendorPdf")
async def parse_new_vendor(
    file: UploadFile = File(...),
    background_tasks: BackgroundTasks = BackgroundTasks()
):
    service = ImportService("new_vendor", parse_new_vendor_pdf)
    return service.handle_import(file.filename, await file.read(), background_tasks)
```

3. Add test file to `Samples/NewVendor/`

4. Add test to `tests/test_all_samples.py`

### Update CodesNames Mapping

```sql
-- Add product mappings
INSERT INTO `lacriee.PROD.CodesNames` (Vendor, Code, Name, Categorie)
VALUES
  ('laurent_daniel', 'LD_SAUMON_E', 'Saumon Écossais Extra', 'SAUMON'),
  ('vvqm', 'VVQ_TURBOT_1', 'Turbot 1-2kg', 'POISSON BLANC');

-- Check unknown products to map
SELECT * FROM `lacriee.PROD.v_products_to_map`
ORDER BY occurrence_count DESC
LIMIT 50;
```

---

## 📚 Documentation

| Document | Description |
|----------|-------------|
| **[TESTING.md](TESTING.md)** | 🧪 **Complete testing procedures** |
| [docs/ARCHITECTURE_PRO.md](docs/ARCHITECTURE_PRO.md) | Detailed architecture and design |
| [docs/PROJET_FINAL.md](docs/PROJET_FINAL.md) | Project summary and status |
| [docs/TESTS_RESULTS.md](docs/TESTS_RESULTS.md) | Test results and bug fixes |
| [docs/PHASE1_READY.md](docs/PHASE1_READY.md) | Phase 1 implementation guide |

---

## 🐛 Troubleshooting

### Container Won't Start

```bash
# Check logs
docker logs fastapi-pdf-parser

# Rebuild image
docker-compose down && docker-compose up -d --build
```

### Test Fails: "No module named 'main'"

```bash
# Ensure working directory is /app
docker exec fastapi-pdf-parser pwd
# Should output: /app
```

### BigQuery: "Streaming buffer rows cannot be modified"

This is **expected** behavior. Wait 1-2 minutes and verify data manually:

```sql
SELECT COUNT(*) FROM `lacriee.PROD.ProvidersPrices`
WHERE job_id = 'YOUR_JOB_ID';
```

### Parser Error: "Date not found"

```bash
# Debug the specific parser
docker exec fastapi-pdf-parser python tests/test_vvqm_debug.py
docker exec fastapi-pdf-parser python tests/test_demarne_debug.py
```

See **[TESTING.md](TESTING.md)** for more troubleshooting steps.

---

## 🔐 Security

- API requires `x-api-key` header (configure in environment)
- BigQuery credentials via service account JSON
- GCS bucket access via service account
- No secrets in code (environment variables only)

---

## 📊 Monitoring

### Check Import Jobs

```sql
SELECT
  job_id,
  vendor,
  filename,
  status,
  rows_extracted,
  rows_inserted_prod,
  created_at
FROM `lacriee.PROD.ImportJobs`
ORDER BY created_at DESC
LIMIT 10;
```

### Daily Summary

```sql
SELECT * FROM `lacriee.PROD.v_daily_import_summary`
ORDER BY last_import_at DESC;
```

### Failed Jobs

```sql
SELECT * FROM `lacriee.PROD.v_failed_jobs`;
```

---

## 🚀 Production Deployment

1. **Environment Setup:**
   - Create GCP project
   - Create BigQuery dataset `lacriee.PROD`
   - Create GCS bucket `lacriee-archives`
   - Create service account with BigQuery + GCS permissions

2. **Initialize Database:**
   ```bash
   bq query --use_legacy_sql=false < scripts/init_db.sql
   ```

3. **Deploy Container:**
   ```bash
   docker-compose up -d
   ```

4. **Run Tests:**
   ```bash
   docker exec fastapi-pdf-parser python tests/test_all_samples.py
   ```

5. **Verify:**
   - Check ImportJobs table
   - Check ProvidersPrices table
   - Check GCS bucket

---

## 📝 License

Internal BEO ERP Project - Proprietary

---

## 👥 Contributors

- Claude Sonnet 4.5 (Architecture & Implementation)
- Tisse (Product Owner & Testing)

---

## 📞 Support

- **Documentation:** See `docs/` folder
- **Testing:** See [TESTING.md](TESTING.md)
- **Issues:** Check logs with `docker logs fastapi-pdf-parser`

---

**Last Updated:** 2026-01-12
**Version:** 1.0.0
**Status:** ✅ Production Ready (3/4 vendors tested)
