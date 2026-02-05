# Documentation Technique LaCriee

## 0. Configuration Docker Local

Le fichier `docker-compose.yml` monte le service account pour les credentials GCP:
```yaml
volumes:
  - ./config/lacrieeparseur.json:/google_credentials/service_account.json:ro
environment:
  - GOOGLE_APPLICATION_CREDENTIALS=/google_credentials/service_account.json
```

**Important**: Le fichier `config/lacrieeparseur.json` contient la clé privée du service account. Ne jamais commiter ce fichier.

### Commandes Docker Détaillées

```bash
# Démarrer les conteneurs
docker-compose up -d

# Voir les logs
docker logs fastapi-pdf-parser

# Rebuild après changement de dépendances
docker-compose down && docker-compose up -d --build

# Test d'un parseur spécifique
docker exec fastapi-pdf-parser python -c "
from parsers import vvqm
data = vvqm.parse(open('Samples/VVQM/GEXPORT.pdf', 'rb').read(), harmonize=True)
print(len(data), 'produits')
"

# Accéder au shell du conteneur
docker exec -it fastapi-pdf-parser bash
```

---

## 1. Pipeline AllPrices

### Vue d'ensemble

```
Fichier PDF/Excel -> Parser -> Harmonisation -> load_to_all_prices -> MERGE -> AllPrices
```

### Format keyDate par parseur

| Parser | Format | Exemple |
|--------|--------|---------|
| Audierne | `AUD_{ProductName}_{Date}` | `AUD_BAR_LIGNE_250101` |
| Demarne | `{Code}_{Date}` | `12345_2026-01-15` |
| Hennequin | `HNQ_{ProductName}_{Date}` | `HNQ_bar_pt_bateau_250114` |
| Laurent Daniel | `LD_{Produit}_{Date}` | `LD_BAR_LIGNE_250115` |
| VVQM | `VVQM_{Produit}_{Calibre}_{Date}` | `VVQM_BAR_1/2_2026-01-15` |

### Processus load_to_all_prices

1. Construit les rows avec schéma explicite
2. Charge dans table staging temporaire
3. MERGE vers AllPrices (UPDATE si key_date existe, INSERT sinon)
4. Supprime staging

---

## 2. Schema BigQuery - AllPrices

```sql
CREATE TABLE AllPrices (
    key_date            STRING,       -- Clé unique
    date                DATE,
    vendor              STRING,
    code_provider       STRING,
    product_name        STRING,
    prix                FLOAT64,

    -- Attributs harmonisés
    categorie           STRING,       -- BAR, SOLE, TURBOT...
    methode_peche       STRING,       -- LIGNE, PB, CHALUT...
    qualite             STRING,       -- EXTRA, SUP, PREMIUM...
    decoupe             STRING,       -- FILET, DOS, QUEUE...
    etat                STRING,       -- VIDE, VIVANT, CUIT...
    origine             STRING,       -- FRANCE, ECOSSE, BRETAGNE...
    calibre             STRING,       -- 1/2, 500/600, T2...

    -- Attributs extraits
    type_production     STRING,       -- SAUVAGE, ELEVAGE
    couleur             STRING,       -- ROUGE, BLANCHE, NOIRE

    -- Attributs spécifiques
    conservation        STRING,       -- FRAIS, CONGELE (Hennequin)
    trim                STRING,       -- TRIM_C, TRIM_D (Audierne)
    label               STRING,       -- MSC, BIO, ASC (Demarne)
    variante            STRING,       -- Demarne
    colisage            STRING,       -- Colisage du produit (Demarne)
    unite_facturee      STRING,       -- Unité de facturation (Demarne)

    -- Métadonnées
    created_at          TIMESTAMP,
    updated_at          TIMESTAMP,
    last_job_id         STRING
);
```

---

## 3. Harmonisation des Attributs

### Module: services/harmonize.py

```python
from services.harmonize import (
    harmonize_product,      # dict -> dict harmonisé
    harmonize_products,     # list[dict] -> list[dict] harmonisés
    normalize_categorie,
    normalize_methode_peche,
    normalize_etat,
    normalize_origine,
    normalize_qualite,
    normalize_calibre,
)
```

### Mapping des colonnes

| Colonne finale | Source | Parseurs concernés |
|----------------|--------|-------------------|
| `categorie` | Categorie | Tous |
| `methode_peche` | Methode_Peche | Tous |
| `qualite` | Qualite | Audierne, Hennequin, LD |
| `decoupe` | Decoupe | Audierne, Hennequin, VVQM, LD |
| `etat` | Etat | Tous |
| `origine` | Origine | Tous sauf VVQM |
| `calibre` | Calibre | Tous |
| `conservation` | Conservation | Hennequin |
| `trim` | Trim | Audierne |
| `label` | Label | Demarne |
| `colisage` | Colisage | Demarne |
| `unite_facturee` | Unite_Facturee | Demarne |
| `type_production` | _(extrait)_ | Hennequin, Demarne |
| `couleur` | _(extrait)_ | Laurent Daniel |

### Harmonisation Categorie

```
BAR                   <- BAR
CARRELET              <- CARRELET, PLIE/ CARRELET
CRUSTACES             <- CRUSTACES BRETONS, CRUSTACES CUITS PAST
LIEU JAUNE            <- LIEU JAUNE, LIEU
SAINT PIERRE          <- SAINT PIERRE, ST PIERRE
SAUMON                <- SAUMON, SAUMONS
```

### Harmonisation Methode_Peche

```
LIGNE                 <- LIGNE, "LIGNE IKEJIME" (extraire LIGNE)
PB                    <- PB, PT BATEAU, PETIT BATEAU
CHALUT                <- CHALUT
SENNEUR               <- SENNEUR
FILEYEUR              <- FILEYEUR
PLONGEE               <- PLONGEE
```

**Règles spéciales:**
- `PT BATEAU` → `PB`
- `LIGNE IKEJIME` → conservé tel quel dans `methode_peche`
- `SAUVAGE` → déplacer vers `type_production=SAUVAGE`

### Harmonisation Qualite

```
EXTRA                 <- EXTRA
EXTRA PINS            <- EXTRA PINS
SUP                   <- SUP
PREMIUM               <- PREMIUM, QUALITE PREMIUM
BIO                   <- BIO
XX                    <- XX (Laurent Daniel)
SF                    <- SF (Sans Flanc - Laurent Daniel)
```

### Harmonisation Decoupe

```
FILET                 <- FILET, FT
DOS                   <- DOS
QUEUE                 <- QUEUE
AILE                  <- AILE
DARNE                 <- DARNE
PAVE                  <- PAVE
LONGE                 <- LONGE
BLANC                 <- BLANC (blanc de seiche)
```

### Harmonisation Etat

```
VIDE                  <- VIDEE, VIDÉ, VIDE
VIVANT                <- VIVANT
PELE                  <- PELEE, PELÉE, PELE
CUIT                  <- CUIT
CORAILLE              <- CORAILLEES, CORAIL
DESARETE              <- DESARETEE
ENTIER                <- ENTIÈRE, ENTIER
```

**Règle spéciale (Laurent Daniel):**
- `ROUGE`, `BLANCHE`, `NOIRE` → nouveau champ `couleur`

### Harmonisation Origine

```
# Pays
FRANCE, ECOSSE, NORVEGE, IRLANDE, DANEMARK, ILES FEROE, MADAGASCAR, VIETNAM, EQUATEUR

# Régions françaises (garder le détail)
BRETAGNE              <- BRETON, BRETAGNE
VENDEE, ROSCOFF, GLENAN, CANCALE, AUDIERNE

# Zones maritimes
ATLANTIQUE            <- ATLANTIQUE, VAT
FAO27, FAO87          <- garder pour traçabilité
```

**Règle spéciale:**
- `AQUACULTURE` → `type_production=ELEVAGE`
- `SAUVAGE` → `type_production=SAUVAGE`

### Harmonisation Calibre

```
# Normaliser le séparateur décimal
1,5/2   → 1.5/2

# Normaliser le format "plus"
500/+   → 500+
+2      → 2+

# Garder tels quels
T1, T2, T11...        (T-series Audierne)
N°2, N°3, N°4...      (huîtres)
JUMBO, XXL, GEANT...  (Hennequin)
```

---

## 4. Attributs Extraits

### type_production

```
SAUVAGE               <- SAUVAGE (Hennequin methode_peche)
ELEVAGE               <- AQUACULTURE, AQ (Audierne origine)
```

### couleur

```
ROUGE, BLANCHE, NOIRE <- extraits de Etat (Laurent Daniel, crustacés)
```

---

## 5. Spécificités Demarne

### Extraction depuis catégorie composite

```
SAUMON SUPÉRIEUR NORVÈGE
    → categorie: SAUMON
    → qualite: SUP
    → origine: NORVÈGE

BAR SAUVAGE
    → categorie: BAR
    → type_production: SAUVAGE

CREVETTE SAUVAGE CUITE
    → categorie: CREVETTES
    → type_production: SAUVAGE
    → etat: CUIT
```

### Huîtres par marque

Toutes normalisées vers `categorie=HUITRES`:
```
LA BELON, LA CELTIQUE, LA FINE, LA PERLE NOIRE, LA SPECIALE,
PLATE DE BRETAGNE, KYS, ÉTOILE, HUITRE DE NORMANDIE
```

### Labels reconnus

MSC, BIO, ASC, LABEL ROUGE, IGP, AOP

### Trims saumon

`Trim B` → `TRIM_B`, `Trim D` → `TRIM_D`, `Trim E` → `TRIM_E`

### Nettoyage origines

Filtrer les poids (`200 grs`, `1 kg` → NULL)

---

## 6. Conservation (Hennequin)

```
FRAIS                 <- FRAIS
CONGELE               <- CONGELEE, CONGELE
SURGELE               <- SURGELEE, SURGELE
IQF                   <- IQF
```

---

## 7. Trim (Audierne - Saumon)

```
TRIM_C                <- TRIM C
TRIM_D                <- TRIM D
TRIM_E                <- TRIM E
```

---

## 8. Module Storage (services/storage.py)

### Fonctions disponibles

```python
from services.storage import (
    archive_file,          # Archiver un fichier dans GCS
    download_file,         # Télécharger un fichier depuis GCS
    generate_signed_url,   # Générer une URL signée temporaire
)
```

### generate_signed_url

Génère une URL signée pour accès temporaire à un fichier GCS.

```python
url = generate_signed_url(
    gcs_url="gs://lacriee-archives/VVQM/2026-01-30/fichier.pdf",
    expiration_minutes=60  # défaut: 60
)
# Retourne: https://storage.googleapis.com/...?X-Goog-Signature=...
```

**Comportement:**
- En local (avec fichier de clé SA): signature directe via la clé privée
- Sur Cloud Run: signature via IAM API (nécessite rôle `serviceAccountTokenCreator`)

---

## 9. Tables BigQuery

| Table | Description |
|-------|-------------|
| `PROD.AllPrices` | Table unifiée des prix (MERGE) |
| `PROD.ImportJobs` | Tracking des jobs (status, metrics) |
| `PROD.UnknownProducts` | Produits non mappés |
| `PROD.DemarneStructured` | Debug Demarne |
| `PROD.HennequinStructured` | Debug Hennequin |

---

## 9bis. Vues BigQuery de Mapping (Correction Qualité)

**IMPORTANT**: Pour corriger des erreurs de catégorie ou calibre, modifier ces vues directement dans BigQuery (pas de code Python à changer).

**📖 Guide complet**: Voir [MAPPING_BIGQUERY.md](MAPPING_BIGQUERY.md) pour la documentation détaillée du système de mapping.

### `PROD.Mapping_Categories` (VUE)

Mapping `categorie_raw` + `decoupe` (optionnel) → `famille_std` + `espece_std`.

**Structure avec colonne decoupe** (depuis 2026-02):
```sql
-- Structure: UNNEST avec ~150 entrées
STRUCT('BAR' AS categorie_raw, CAST(NULL AS STRING) AS decoupe, 'POISSON' AS famille_std, 'BAR' AS espece_std),

-- Exemple de mapping conditionnel par découpe:
STRUCT('ANCHOIS', CAST(NULL AS STRING), 'POISSON', 'ANCHOIS'),  -- Anchois frais
STRUCT('ANCHOIS', 'FILET', 'EPICERIE', 'EPICERIE'),              -- Filets marinés
```

**Logique**:
- `decoupe = NULL`: Mapping par défaut pour la catégorie
- `decoupe = 'FILET'`: Mapping spécifique si decoupe='FILET'
- Les matchs spécifiques ont priorité sur les matchs génériques

**Utilisée par**:
- `sp_Update_Analytics_Produits_Comparaison` (stored procedure principale)
- `Mapping_Calibres`
- `V_Prix_Du_Jour`

**Accès rapide** (via Docker):
```bash
docker exec fastapi-pdf-parser python -c "
from google.cloud import bigquery
client = bigquery.Client(project='lacriee')
t = client.get_table('lacriee.PROD.Mapping_Categories')
print(t.view_query)
"
```

### `PROD.Mapping_Calibres` (VUE)

Vue dynamique qui parse automatiquement les calibres depuis `AllPrices`.

**Colonnes produites**: `espece_std`, `calibre_raw`, `unite_std`, `min_val`, `max_val`

**Unités détectées**:
- `GRAMMES` (défaut) - ex: `500/800`, `1.2/1.5`
- `NUMERO` (huîtres) - ex: `N°2`, `N°3`
- `PIECES/KG` (St-Jacques, grenouilles, petits crustacés)

**Logique de parsing**:
1. Nettoie le texte brut (regex)
2. Détecte l'unité selon espece_std et patterns
3. Calcule min/max (gère les plages `X/Y`, les `+`, conversions kg→g)

**Pour corriger un calibre mal parsé**: Modifier les CASE WHEN dans la vue.

### `PROD.sp_Update_Analytics_Produits_Comparaison` (PROCEDURE)

Stored procedure qui recalcule la table `Analytics_Produits_Comparaison` (table matérialisée).

**Jointure avec Mapping_Categories** (avec logique de priorité decoupe):
```sql
LEFT JOIN `lacriee.PROD.Mapping_Categories` cat
  ON TRIM(UPPER(p.categorie)) = TRIM(UPPER(cat.categorie_raw))
  AND (
    (cat.decoupe IS NOT NULL AND TRIM(UPPER(COALESCE(p.decoupe, ''))) = TRIM(UPPER(cat.decoupe)))
    OR (cat.decoupe IS NULL)
  )
QUALIFY ROW_NUMBER() OVER(
  PARTITION BY p.code_provider
  ORDER BY CASE WHEN cat.decoupe IS NOT NULL THEN 1 ELSE 2 END
) = 1
```

**Exécution**:
```sql
CALL `lacriee.PROD.sp_Update_Analytics_Produits_Comparaison`();
```

### `PROD.V_Prix_Du_Jour` (VUE)

Vue alternative qui joint `AllPrices` + `Mapping_Categories` + `Mapping_Calibres` pour:
- Enrichir avec `famille_std`, `espece_std`
- Calculer les flags de calibre (`calib500`, `calib1000`...)
- Calculer les rankings par calibre

**Note**: Moins utilisée que `Analytics_Produits_Comparaison` (table matérialisée)

---

## 10. Scripts Utilitaires

```bash
# Vérifier les counts par vendor
docker exec -e PYTHONPATH=/app fastapi-pdf-parser python scripts/check_counts.py

# Vider AllPrices (attention!)
docker exec -e PYTHONPATH=/app fastapi-pdf-parser python scripts/clear_all_prices.py
```

---

## 11. Statistiques par Parseur

| Parseur | Produits (sample) | Attributs spécifiques |
|---------|-------------------|----------------------|
| Audierne | 174 | trim |
| Hennequin | 103 | conservation, type_production |
| VVQM | 89 | - |
| Laurent Daniel | 145 | couleur |
| Demarne | 764 | label, variante, type_production |
