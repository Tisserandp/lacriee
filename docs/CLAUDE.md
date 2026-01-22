# Documentation Technique LaCriee

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
    technique_abattage  STRING,       -- IKEJIME
    couleur             STRING,       -- ROUGE, BLANCHE, NOIRE

    -- Attributs spécifiques
    conservation        STRING,       -- FRAIS, CONGELE (Hennequin)
    trim                STRING,       -- TRIM_C, TRIM_D (Audierne)
    label               STRING,       -- MSC, BIO, ASC (Demarne)
    variante            STRING,       -- Demarne

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
| `type_production` | _(extrait)_ | Hennequin, Demarne |
| `technique_abattage` | _(extrait)_ | VVQM |
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
- `LIGNE IKEJIME` → `methode_peche=LIGNE` + `technique_abattage=IKEJIME`
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

### technique_abattage

```
IKEJIME               <- extrait de "LIGNE IKEJIME" (VVQM)
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

## 8. Tables BigQuery

| Table | Description |
|-------|-------------|
| `PROD.AllPrices` | Table unifiée des prix (MERGE) |
| `PROD.ImportJobs` | Tracking des jobs (status, metrics) |
| `PROD.UnknownProducts` | Produits non mappés |
| `PROD.DemarneStructured` | Debug Demarne |
| `PROD.HennequinStructured` | Debug Hennequin |

---

## 9. Scripts Utilitaires

```bash
# Charger tous les échantillons
docker exec -e PYTHONPATH=/app fastapi-pdf-parser python scripts/load_samples.py

# Vérifier les counts par vendor
docker exec -e PYTHONPATH=/app fastapi-pdf-parser python scripts/check_counts.py

# Vider AllPrices (attention!)
docker exec -e PYTHONPATH=/app fastapi-pdf-parser python scripts/clear_all_prices.py
```

---

## 10. Statistiques par Parseur

| Parseur | Produits (sample) | Attributs spécifiques |
|---------|-------------------|----------------------|
| Audierne | 174 | trim |
| Hennequin | 103 | conservation, type_production |
| VVQM | 89 | technique_abattage |
| Laurent Daniel | 145 | couleur |
| Demarne | 764 | label, variante, type_production |
