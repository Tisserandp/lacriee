# LaCriee - Pipeline ELT Poissonnerie

## Contexte
Pipeline ELT qui parse des fichiers PDF/Excel de fournisseurs de produits de la mer, harmonise les données et les charge dans BigQuery.

## Stack
- **API**: FastAPI + Docker (Cloud Run)
- **Parsing**: PyMuPDF (PDF), openpyxl (Excel)
- **Storage**: GCS (archives), BigQuery (données)
- **Orchestration**: n8n (appelle les endpoints)

## Les 5 Parseurs

| Parseur | Format | Endpoint | Fichier |
|---------|--------|----------|---------|
| Laurent Daniel | PDF | `/parseLaurentDpdf` | `parsers/laurent_daniel.py` |
| VVQM | PDF | `/parseVVQpdf` | `parsers/vvqm.py` |
| Demarne | Excel | `/parseDemarneXLS` | `parsers/demarne.py` |
| Hennequin | PDF | `/parseHennequinPDF` | `parsers/hennequin.py` |
| Audierne | PDF | `/parseAudiernepdf` | `parsers/audierne.py` |

Tous les parseurs ont la signature: `parse(file_bytes, harmonize=True, **kwargs) -> list[dict]`

## Structure Critique

```
parsers/              # 5 parseurs autonomes
services/
  harmonize.py        # Mappings de normalisation (SOURCE DE VERITE)
  bigquery.py         # Chargement BQ + MERGE
  import_service.py   # Orchestration (archive GCS + job tracking + parsing async)
  storage.py          # Archivage GCS
main.py               # Endpoints FastAPI
scripts/init_db.sql   # Schema BigQuery
Samples/              # Fichiers de test par vendor
```

## Pipeline de Traitement

```
POST /parse{Vendor} (fichier)
    |
    +-- SYNC (<1s): archive GCS + create job -> return job_id
    |
    +-- ASYNC (background):
        parse() -> harmonize() -> load_to_all_prices() -> MERGE BigQuery
```

## Conventions

- **Attributs**: snake_case (`methode_peche`, `key_date`)
- **Valeurs**: MAJUSCULES sans accents (`VIDE`, `PELE`, `SAINT PIERRE`)
- **keyDate**: Clé unique par parseur (`{Vendor}_{Code}_{Date}`) sauf pour le parseur Demarne ou c'est simplement (`{Code}_{Date}`)

## Commandes Fréquentes

```bash
# Tests
docker exec fastapi-pdf-parser python -m pytest tests/test_all_samples.py -v

# Test d'un parseur
docker exec fastapi-pdf-parser python -c "
from parsers import vvqm
data = vvqm.parse(open('Samples/VVQM/GEXPORT.pdf', 'rb').read(), harmonize=True)
print(len(data), 'produits')
"

# Charger les samples
docker exec -e PYTHONPATH=/app fastapi-pdf-parser python scripts/load_samples.py
```

## Points d'Attention

1. **Streaming buffer BigQuery**: 1-2 min de délai après insert avant visibilité
2. **harmonize.py**: Ne pas modifier les mappings sans validation (affecte tous les parseurs)
3. **init_db.sql**: Backup avant modification du schema
4. **Samples/**: Toujours tester avec les fichiers d'exemple avant prod

## Contrat JSON n8n

**Réponse immédiate**:
```json
{"job_id": "...", "status": "processing", "check_status_url": "/jobs/{job_id}"}
```

**Status check** (`GET /jobs/{job_id}`):
```json
{"status": "completed", "metrics": {"rows_extracted": 96, "rows_inserted_prod": 96}}
```

## Déploiement Cloud Run

**Production**: https://parsers-847079377265.europe-west1.run.app

```bash
# Déploiement rapide
gcloud run deploy parsers \
  --source . \
  --project=lacriee \
  --region=europe-west1 \
  --allow-unauthenticated \
  --memory=1Gi \
  --timeout=300s \
  --set-env-vars="GCP_PROJECT_ID=lacriee,GCS_BUCKET=lacriee-archives"

# Voir l'URL du service
gcloud run services describe parsers --project=lacriee --region=europe-west1 --format="value(status.url)"

# Logs en temps réel
gcloud run services logs tail parsers --project=lacriee --region=europe-west1
```

**Documentation complète**: [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md)

## Documentation Technique

Voir [docs/CLAUDE.md](docs/CLAUDE.md) pour:
- Mappings d'harmonisation complets
- Schema BigQuery détaillé
- Workflow AllPrices
