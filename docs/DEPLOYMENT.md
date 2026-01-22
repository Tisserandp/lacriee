# Déploiement Cloud Run - Service "parsers"

## Vue d'Ensemble

Service FastAPI déployé sur Google Cloud Run pour parser des fichiers PDF/Excel de fournisseurs.

- **Service**: `parsers`
- **Projet GCP**: `lacriee`
- **Région**: `europe-west1` (Belgique)
- **URL**: https://parsers-847079377265.europe-west1.run.app
- **Mémoire**: 1Gi
- **Authentification**: Allow unauthenticated (API key dans headers)

## Pré-requis

- gcloud CLI installé et authentifié
- Accès au projet GCP `lacriee`
- Secrets configurés dans Secret Manager

## Déploiement Rapide

### 1. Depuis le répertoire racine
```bash
cd c:\Users\Tisse\OneDrive\Tisserandp\LaCriee
```

### 2. Déployer sur Cloud Run
```bash
gcloud run deploy parsers \
  --source . \
  --project=lacriee \
  --region=europe-west1 \
  --platform=managed \
  --allow-unauthenticated \
  --memory=1Gi \
  --cpu=1 \
  --timeout=300s \
  --max-instances=10 \
  --min-instances=0 \
  --concurrency=80 \
  --set-env-vars="GCP_PROJECT_ID=lacriee,GCS_BUCKET=lacriee-archives"
```

### 3. Récupérer l'URL
```bash
gcloud run services describe parsers \
  --project=lacriee \
  --region=europe-west1 \
  --format="value(status.url)"
```

## Configuration

### Variables d'Environnement

Les variables suivantes sont configurées au déploiement:

- `GCP_PROJECT_ID=lacriee` - Projet GCP
- `GCS_BUCKET=lacriee-archives` - Bucket d'archivage
- `PORT=8080` - Défini dans le Dockerfile

### Secrets (Secret Manager)

Le service utilise les secrets suivants:

- `PDF_PARSER_API_KEY` - Clé d'authentification pour les endpoints

Récupérer un secret:
```bash
gcloud secrets versions access latest \
  --secret=PDF_PARSER_API_KEY \
  --project=lacriee
```

### Permissions IAM

Le service utilise le compte de service par défaut:
`847079377265-compute@developer.gserviceaccount.com`

Permissions nécessaires:
- `roles/bigquery.dataEditor` - Accès au dataset BigQuery
- `roles/storage.objectAdmin` - Archivage dans GCS
- `roles/secretmanager.secretAccessor` - Accès aux secrets

## Endpoints Disponibles

Tous les endpoints nécessitent le header `X-API-Key: {valeur du secret}`:

- `POST /parseLaurentDpdf` - Laurent Daniel PDF
- `POST /parseVVQpdf` - VVQM PDF
- `POST /parseDemarneXLS` - Demarne Excel
- `POST /parseHennequinPDF` - Hennequin PDF
- `POST /parseAudiernepdf` - Audierne PDF
- `GET /jobs/{job_id}` - Vérifier statut d'un job
- `GET /test-parser` - Interface de test HTML

## Gestion du Service

### Voir les Logs

Logs en temps réel:
```bash
gcloud run services logs tail parsers \
  --project=lacriee \
  --region=europe-west1
```

Logs des dernières 24h:
```bash
gcloud run services logs read parsers \
  --project=lacriee \
  --region=europe-west1 \
  --limit=50
```

### Rollback

Lister les révisions:
```bash
gcloud run revisions list \
  --service=parsers \
  --project=lacriee \
  --region=europe-west1
```

Revenir à une révision précédente:
```bash
gcloud run services update-traffic parsers \
  --to-revisions=parsers-00001-xxx=100 \
  --project=lacriee \
  --region=europe-west1
```

### Mise à Jour des Variables d'Environnement

```bash
gcloud run services update parsers \
  --update-env-vars="NOUVELLE_VAR=valeur" \
  --project=lacriee \
  --region=europe-west1
```

### Scaling Vertical (Plus de Mémoire)

Si erreurs OOM, augmenter à 2Gi:
```bash
gcloud run services update parsers \
  --memory=2Gi \
  --project=lacriee \
  --region=europe-west1
```

## Monitoring

### Métriques

Console GCP > Cloud Run > parsers > Métriques:
- Latence des requêtes
- Utilisation mémoire (surveiller si proche de 1Gi)
- Nombre d'instances actives
- Taux d'erreur

### Alertes Recommandées

Configurer Cloud Monitoring pour:
- Erreurs 5xx
- Latence >10s
- Utilisation mémoire >80%

## Test du Service

### Test Simple
```bash
curl https://parsers-847079377265.europe-west1.run.app/test-parser \
  -H "X-API-Key: VOTRE_CLE_API"
```

### Test avec Fichier
```bash
curl -X POST https://parsers-847079377265.europe-west1.run.app/parseLaurentDpdf \
  -H "X-API-Key: VOTRE_CLE_API" \
  -F "file=@chemin/vers/fichier.pdf"
```

## Sécurité

- API key stockée dans Secret Manager
- Service account avec permissions minimales
- CORS configuré dans [main.py](../main.py)
- Allow-unauthenticated activé (sécurisé par API key)

## Coûts Estimés

- **Compute**: ~$0.024/heure par instance active (1 vCPU, 1Gi RAM)
- **Requêtes**: Gratuit jusqu'à 2M requêtes/mois
- **Scale to zero**: Pas de coûts quand inactif
- **Estimation mensuelle**: $5-20 selon utilisation

## Points d'Attention

### Mémoire 1Gi
Choix économique mais risque d'**OOM (Out Of Memory)** sur:
- Très gros fichiers PDF (>50 pages avec images haute résolution)
- Parsing Excel avec milliers de lignes
- Requêtes simultanées intensives

**Monitoring recommandé**: Si utilisation mémoire >80% de façon régulière, passer à 2Gi.

### CORS Configuration
Dans [main.py](../main.py), le CORS est ouvert:
```python
allow_origins=["*"]
```
Pour production, restreindre aux domaines autorisés.

### Logs
Cloud Run utilise **stdout/stderr** pour Cloud Logging.
- Les logs fichiers (`/app/logs`) ne persistent PAS entre redémarrages
- Tout doit être loggé via `logger.info()` pour apparaître dans Cloud Logging

### Timeout
Maximum 300s (5 min) par requête. Suffisant pour parsing PDF/Excel.

## Commandes Utiles

### Obtenir le Project Number
```bash
gcloud projects describe lacriee \
  --format="value(projectNumber)"
```

### Voir le Service Account par Défaut
```bash
gcloud iam service-accounts list \
  --project=lacriee \
  --filter="compute@developer"
```

### Tester Localement avec Docker
```bash
# Build l'image
docker build -t parsers-local .

# Run avec env vars
docker run -p 8080:8080 \
  -e GCP_PROJECT_ID=lacriee \
  -e GCS_BUCKET=lacriee-archives \
  -e GOOGLE_APPLICATION_CREDENTIALS=/tmp/keys/sa.json \
  -v ~/.config/gcloud:/tmp/keys:ro \
  parsers-local
```

### Supprimer le Service (Si Nécessaire)
```bash
gcloud run services delete parsers \
  --project=lacriee \
  --region=europe-west1
```

## Troubleshooting

### Erreur: Container failed to start
- Vérifier les logs: `gcloud run services logs tail parsers`
- Vérifier que le Dockerfile utilise `CMD ["python", "-m", "uvicorn", ...]`
- Vérifier que requirements.txt contient bien `uvicorn`

### Erreur: Permission denied
- Vérifier les permissions IAM du service account
- Vérifier que les secrets existent dans Secret Manager

### Erreur: Out of Memory
- Augmenter la mémoire à 2Gi
- Optimiser le code pour réduire l'utilisation mémoire

## Historique des Déploiements

- **2026-01-22**: Déploiement initial
  - Révision `parsers-00003-f7h`
  - Configuration: 1Gi RAM, europe-west1
  - URL: https://parsers-847079377265.europe-west1.run.app
