# LaCriee - Pipeline ELT Poissonnerie

Pipeline ELT qui parse des fichiers PDF/Excel de fournisseurs de produits de la mer et charge les données dans BigQuery.

## Quick Start

```bash
# Démarrer le conteneur
docker-compose up -d

# Vérifier le service
curl http://localhost:8080/health

# Importer un fichier
curl -X POST "http://localhost:8080/parseLaurentDpdf" \
  -H "x-api-key: YOUR_API_KEY" \
  -F "file=@Samples/LaurentD/CC.pdf"

# Vérifier le statut du job
curl "http://localhost:8080/jobs/{job_id}"
```

## Endpoints

| Endpoint | Vendor | Format |
|----------|--------|--------|
| `/parseLaurentDpdf` | Laurent Daniel | PDF |
| `/parseVVQpdf` | VVQM | PDF |
| `/parseDemarneXLS` | Demarne | Excel |
| `/parseHennequinPDF` | Hennequin | PDF |
| `/parseAudiernepdf` | Audierne | PDF |
| `/jobs/{job_id}` | Status check | - |

## Structure du Projet

```
parsers/              # 5 parseurs autonomes
services/
  harmonize.py        # Normalisation des attributs
  bigquery.py         # Chargement BigQuery
  import_service.py   # Orchestration ELT
  storage.py          # Archivage GCS
main.py               # FastAPI endpoints
Samples/              # Fichiers de test
tests/                # Tests
```

## Tests

```bash
# Tests complets (tous les vendors)
docker exec fastapi-pdf-parser python tests/test_all_samples.py

# Test d'intégration (Laurent-Daniel)
docker exec fastapi-pdf-parser python tests/test_direct.py
```

Voir [TESTING.md](TESTING.md) pour les procédures détaillées.

## Configuration

Variables d'environnement (docker-compose.yml):
- `GOOGLE_APPLICATION_CREDENTIALS`: Credentials GCP
- `BQ_DATASET`: Dataset BigQuery (lacriee.PROD)
- `GCS_BUCKET`: Bucket GCS (lacriee-archives)

## Documentation

| Document | Description |
|----------|-------------|
| [CLAUDE.md](CLAUDE.md) | Contexte projet (chargé automatiquement) |
| [docs/CLAUDE.md](docs/CLAUDE.md) | Documentation technique détaillée |
| [TESTING.md](TESTING.md) | Procédures de test |

## Troubleshooting

```bash
# Logs du conteneur
docker logs fastapi-pdf-parser

# Rebuild
docker-compose down && docker-compose up -d --build
```

**Streaming buffer BigQuery**: Les données peuvent mettre 1-2 min à apparaître après insertion.
