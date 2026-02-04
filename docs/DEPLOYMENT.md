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

### Configuration Initiale: Secret PDF_PARSER_API_KEY

**Important**: Le service nécessite la clé API `PDF_PARSER_API_KEY` pour s'authentifier. Cette clé est lue **automatiquement depuis Secret Manager** en production.

#### 1. Créer le secret (une seule fois)

```bash
# Créer le secret avec la clé API
echo -n "ProvidersBEO123" | gcloud secrets create pdf-parser-api-key \
  --project=lacriee \
  --replication-policy=automatic \
  --data-file=-
```

#### 2. Donner accès au service Cloud Run

```bash
# Récupérer le numéro de projet
PROJECT_NUMBER=$(gcloud projects describe lacriee --format="value(projectNumber)")

# Donner accès au service account par défaut
gcloud secrets add-iam-policy-binding pdf-parser-api-key \
  --project=lacriee \
  --member="serviceAccount:${PROJECT_NUMBER}-compute@developer.gserviceaccount.com" \
  --role="roles/secretmanager.secretAccessor"
```

**Note**: Le code dans [config.py](../config.py) lit automatiquement ce secret en production via `get_secret("pdf-parser-api-key")`. En développement local, il utilise la variable d'environnement `PDF_PARSER_API_KEY` du fichier `.env.local`.

## Déploiement Rapide

**IMPORTANT**: Le déploiement avec `--source .` échoue souvent à cause des timestamps OneDrive.
Utiliser la méthode Cloud Build ci-dessous.

### Méthode 1: Cloud Build (Recommandée)

Cette méthode évite les problèmes de timestamps et de fichiers problématiques.

```bash
# 1. Créer un répertoire temporaire propre
cd "c:/Users/Tisse/OneDrive/Tisserandp/LaCriee"
rm -rf /tmp/lacriee_deploy && mkdir -p /tmp/lacriee_deploy

# 2. Copier uniquement les fichiers nécessaires (PAS de .git, secrets, config/, Samples/)
cp -r main.py requirements.txt Dockerfile config.py parsers/ services/ models/ utils/ scripts/ static/ templates/ /tmp/lacriee_deploy/

# 3. Créer le dossier logs et fixer les timestamps
mkdir -p /tmp/lacriee_deploy/logs
find /tmp/lacriee_deploy -type f -exec touch {} \;

# 4. Build avec Cloud Build
gcloud builds submit /tmp/lacriee_deploy --tag gcr.io/lacriee/parsers --project=lacriee

# 5. Déployer l'image
gcloud run deploy parsers \
  --image gcr.io/lacriee/parsers \
  --project=lacriee \
  --region=europe-west1 \
  --allow-unauthenticated \
  --memory=1Gi \
  --timeout=300s \
  --set-env-vars="GCP_PROJECT_ID=lacriee,GCS_BUCKET=lacriee-archives"
```

**Note**: `PDF_PARSER_API_KEY` n'est plus passée via env vars - elle est lue automatiquement depuis Secret Manager.

### Méthode 2: --source (Recommandée, plus rapide)

```bash
gcloud run deploy parsers \
  --source . \
  --project=lacriee \
  --region=europe-west1 \
  --allow-unauthenticated \
  --memory=1Gi \
  --timeout=300s \
  --set-env-vars="GCP_PROJECT_ID=lacriee,GCS_BUCKET=lacriee-archives"
```

**Note**: Cette méthode fonctionne maintenant correctement avec les nouveaux buildpacks.

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

**Clé API (`PDF_PARSER_API_KEY`)**:
- **Production Cloud Run**: Lue automatiquement depuis Secret Manager (`pdf-parser-api-key`)
- **Dev local**: Lue depuis la variable d'environnement dans `.env.local`
- Le code dans [config.py:37-49](../config.py#L37-L49) gère cette logique automatiquement

### Permissions IAM

Le service utilise le compte de service par défaut:
`847079377265-compute@developer.gserviceaccount.com`

Permissions nécessaires:
- `roles/bigquery.dataEditor` - Accès au dataset BigQuery
- `roles/storage.objectAdmin` - Archivage dans GCS
- `roles/iam.serviceAccountTokenCreator` - Génération d'URLs signées (sur lui-même)

### URLs Signées (endpoint /jobs/{job_id}/file)

Pour permettre au service de générer des URLs signées sur Cloud Run, le service account doit pouvoir signer ses propres tokens:

```bash
# Récupérer le service account
SA=$(gcloud run services describe parsers --project=lacriee --region=europe-west1 --format="value(spec.template.spec.serviceAccountName)")

# Ajouter le rôle Service Account Token Creator
gcloud iam service-accounts add-iam-policy-binding $SA \
  --member="serviceAccount:$SA" \
  --role="roles/iam.serviceAccountTokenCreator" \
  --project=lacriee
```

**En local**: Utiliser un fichier de clé de service account (voir docker-compose.yml)

## Endpoints Disponibles

Tous les endpoints nécessitent le header `X-API-Key: {valeur du secret}`:

- `POST /parseLaurentDpdf` - Laurent Daniel PDF
- `POST /parseVVQpdf` - VVQM PDF
- `POST /parseDemarneXLS` - Demarne Excel
- `POST /parseHennequinPDF` - Hennequin PDF
- `POST /parseAudiernepdf` - Audierne PDF
- `GET /jobs/{job_id}` - Vérifier statut d'un job
- `GET /jobs/{job_id}/file` - URL signée pour télécharger le fichier source
- `POST /jobs/{job_id}/replay` - Rejouer un job avec le parseur actuel
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

Pour mettre à jour les variables d'environnement standard:
```bash
gcloud run services update parsers \
  --update-env-vars="GCP_PROJECT_ID=lacriee,GCS_BUCKET=lacriee-archives" \
  --project=lacriee \
  --region=europe-west1
```

**Pour changer l'API key**: Mettre à jour le secret dans Secret Manager (pas de redéploiement nécessaire):
```bash
echo -n "nouvelle_cle" | gcloud secrets versions add pdf-parser-api-key \
  --project=lacriee \
  --data-file=-
```

Le service lira automatiquement la nouvelle version du secret au prochain démarrage d'instance.

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
  -H "X-API-Key: YOUR_API_KEY"
```

### Test avec Fichier
```bash
curl -X POST https://parsers-847079377265.europe-west1.run.app/parseLaurentDpdf \
  -H "X-API-Key: YOUR_API_KEY" \
  -F "file=@chemin/vers/fichier.pdf"
```

## Sécurité

- **API Key**: Passée comme variable d'environnement à Cloud Run (ne jamais commiter en clair)
- **Variables Sensibles**: Stocker dans `.env.local` (ignoré par git)
- Service account avec permissions minimales (BigQuery, GCS)
- CORS configuré dans [main.py](../main.py)
- Allow-unauthenticated activé (sécurisé par API key dans header X-API-Key)

### Gestion des Secrets Localement

Créer un fichier `.env.local` (ignoré par git):
```bash
GCP_PROJECT_ID=lacriee
GCS_BUCKET=lacriee-archives
PDF_PARSER_API_KEY=ProvidersBEO123
```

Ne **JAMAIS** commiter les vraies clés API en clair dans git.

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
  -e PDF_PARSER_API_KEY=YOUR_API_KEY \
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

- **2026-02-04**: Migration vers Secret Manager pour l'API key
  - Révision `parsers-00017-qgk`
  - **Fix critique**: API key maintenant lue depuis Secret Manager (`pdf-parser-api-key`)
  - Modification [config.py](../config.py): `get_api_key()` avec fallback Secret Manager
  - Résolution erreur 500 "PDF_PARSER_API_KEY environment variable not set" en production
  - Plus besoin de passer l'API key via `--set-env-vars` au déploiement

- **2026-02-02**: Ajout endpoints job file/replay
  - Révision `parsers-00015-5pf`
  - Nouveaux endpoints: `/jobs/{job_id}/file`, `/jobs/{job_id}/replay`
  - Fonctions storage: `download_file()`, `generate_signed_url()`
  - Fix: PDF_PARSER_API_KEY ajoutée aux env vars

- **2026-01-23**: Déploiement avec clé API en variable d'environnement
  - Révision `parsers-00007-78z`
  - Configuration: 1Gi RAM, europe-west1
  - Env vars: GCP_PROJECT_ID, GCS_BUCKET, PDF_PARSER_API_KEY
  - URL: https://parsers-847079377265.europe-west1.run.app

- **2026-01-22**: Déploiement initial
  - Révision `parsers-00003-f7h`
  - Configuration: 1Gi RAM, europe-west1
  - URL: https://parsers-847079377265.europe-west1.run.app
