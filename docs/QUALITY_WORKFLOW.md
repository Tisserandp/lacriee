# Workflow d'Analyse Qualité des Données

Guide pour utiliser les endpoints `/analysis/*` afin d'inspecter les données dans AllPrices et améliorer les parseurs.

## Vue d'Ensemble

Les endpoints d'analyse permettent de:
- **Identifier** les champs manquants ou mal renseignés
- **Comparer** la qualité des données entre vendors
- **Détecter** les valeurs incohérentes ou non harmonisées
- **Prioriser** les améliorations des parseurs

**Important**: Toujours filtrer par date récente (`date_from=2026-01-26`) car l'historique a été chargé avec harmonisation minimale.

---

## Endpoints Disponibles

### 1. Couverture des Champs par Vendor

```bash
# Analyser la couverture d'un vendor
curl "http://localhost:8080/analysis/coverage?vendor=Demarne" \
  -H "X-API-Key: $PDF_PARSER_API_KEY"

# Filtrer par date récente (recommandé)
curl "http://localhost:8080/analysis/coverage?vendor=VVQM&date_from=2026-01-26" \
  -H "X-API-Key: $PDF_PARSER_API_KEY"
```

**Retour:**
```json
{
  "vendor": "Demarne",
  "total_rows": 764,
  "date_range": {"from": "2026-01-26", "to": "2026-02-04"},
  "coverage": {
    "categorie": {"filled": 764, "empty": 0, "percentage": 100.0},
    "methode_peche": {"filled": 120, "empty": 644, "percentage": 15.7},
    "qualite": {"filled": 450, "empty": 314, "percentage": 58.9},
    "decoupe": {"filled": 320, "empty": 444, "percentage": 41.9},
    "etat": {"filled": 89, "empty": 675, "percentage": 11.6},
    "origine": {"filled": 700, "empty": 64, "percentage": 91.6},
    "calibre": {"filled": 550, "empty": 214, "percentage": 72.0}
  }
}
```

**Interpretation:**
- `percentage < 50%` → Champ potentiellement mal extrait
- `percentage < 10%` → Champ probablement absent du fichier source

---

### 2. Distribution des Valeurs d'un Champ

```bash
# Analyser les valeurs d'un champ
curl "http://localhost:8080/analysis/values/categorie?vendor=Demarne&date_from=2026-01-26" \
  -H "X-API-Key: $PDF_PARSER_API_KEY"

# Limite et tri
curl "http://localhost:8080/analysis/values/qualite?vendor=VVQM&limit=20&sort=count" \
  -H "X-API-Key: $PDF_PARSER_API_KEY"
```

**Retour:**
```json
{
  "field": "categorie",
  "vendor": "Demarne",
  "total_rows": 764,
  "unique_values": 45,
  "top_values": [
    {"value": "SAUMON", "count": 120, "percentage": 15.7},
    {"value": "BAR", "count": 89, "percentage": 11.6},
    {"value": "HUITRES", "count": 78, "percentage": 10.2},
    {"value": null, "count": 0, "percentage": 0.0}
  ]
}
```

**Cas d'usage:**
- Détecter valeurs non harmonisées (ex: "SAUMONS" vs "SAUMON")
- Identifier valeurs brutes non normalisées (ex: "SAUMON SUPERIEUR NORVEGE")
- Repérer valeurs NULL à investiguer

---

### 3. Comparaison entre Vendors

```bash
# Comparer tous les vendors
curl "http://localhost:8080/analysis/compare-vendors?date_from=2026-01-26" \
  -H "X-API-Key: $PDF_PARSER_API_KEY"

# Comparer un sous-ensemble
curl "http://localhost:8080/analysis/compare-vendors?vendors=VVQM,Demarne,Audierne" \
  -H "X-API-Key: $PDF_PARSER_API_KEY"
```

**Retour:**
```json
{
  "comparison": [
    {
      "vendor": "Audierne",
      "total_rows": 174,
      "coverage": {
        "categorie": 100.0,
        "methode_peche": 85.0,
        "qualite": 92.0,
        "decoupe": 67.0,
        "etat": 78.0,
        "origine": 88.0,
        "calibre": 95.0,
        "trim": 45.0
      }
    },
    {
      "vendor": "Demarne",
      "total_rows": 764,
      "coverage": {
        "categorie": 100.0,
        "methode_peche": 15.7,
        "qualite": 58.9,
        "decoupe": 41.9,
        "etat": 11.6,
        "origine": 91.6,
        "calibre": 72.0,
        "label": 22.0
      }
    }
  ],
  "date_range": {"from": "2026-01-26", "to": "2026-02-04"}
}
```

**Interpretation:**
- Identifier les vendors avec faible couverture
- Prioriser les parseurs à améliorer
- Comparer les attributs spécifiques (trim, label, conservation)

---

## Workflow d'Amélioration

### Étape 1: Identifier les Gaps

```bash
# 1. Comparer tous les vendors
curl "http://localhost:8080/analysis/compare-vendors?date_from=2026-01-26" \
  -H "X-API-Key: $KEY" > comparison.json

# 2. Identifier le vendor avec la plus faible couverture
cat comparison.json | jq '.comparison | sort_by(.coverage.methode_peche) | .[0]'
```

### Étape 2: Analyser le Vendor Ciblé

```bash
# 3. Analyser la couverture détaillée
curl "http://localhost:8080/analysis/coverage?vendor=Demarne&date_from=2026-01-26" \
  -H "X-API-Key: $KEY"

# 4. Examiner les valeurs brutes du champ problématique
curl "http://localhost:8080/analysis/values/methode_peche?vendor=Demarne&date_from=2026-01-26" \
  -H "X-API-Key: $KEY"
```

### Étape 3: Investiguer dans BigQuery

```sql
-- Voir les produits avec methode_peche NULL
SELECT product_name, categorie, origine, date
FROM `lacriee.PROD.AllPrices`
WHERE vendor = 'Demarne'
  AND methode_peche IS NULL
  AND date >= '2026-01-26'
LIMIT 50;
```

### Étape 4: Améliorer le Parseur

1. **Localiser le fichier source** dans `Samples/Demarne/`
2. **Identifier** où se trouve l'info manquante (nom produit, colonne spécifique)
3. **Modifier** `parsers/demarne.py` pour extraire le champ
4. **Tester** avec `/test-parser demarne`
5. **Valider** avec l'endpoint analysis

### Étape 5: Valider l'Amélioration

```bash
# 1. Recharger le sample
curl -X POST "http://localhost:8080/parseDemarneXLS" \
  -H "X-API-Key: $KEY" \
  -F "file=@Samples/Demarne/Classeur1 G19.xlsx"

# 2. Attendre 2 min (streaming buffer BigQuery)
sleep 120

# 3. Vérifier la couverture améliorée
curl "http://localhost:8080/analysis/coverage?vendor=Demarne&date_from=2026-02-04" \
  -H "X-API-Key: $KEY"
```

---

## Cas d'Usage Courants

### Cas 1: Détecter valeurs non harmonisées

```bash
# Examiner les valeurs brutes de "etat"
curl "http://localhost:8080/analysis/values/etat?vendor=VVQM&limit=50" \
  -H "X-API-Key: $KEY"

# Si retour contient "VIDEE", "VIDÉ", "VIDE" → améliorer harmonize.py
```

### Cas 2: Comparer qualité avant/après amélioration

```bash
# Avant modification
curl "http://localhost:8080/analysis/coverage?vendor=Hennequin&date_from=2026-01-26&date_to=2026-01-30" \
  -H "X-API-Key: $KEY" > before.json

# Améliorer le parseur, recharger, attendre 2 min

# Après modification
curl "http://localhost:8080/analysis/coverage?vendor=Hennequin&date_from=2026-02-04" \
  -H "X-API-Key: $KEY" > after.json

# Comparer
diff <(jq .coverage before.json) <(jq .coverage after.json)
```

### Cas 3: Identifier produits à mapper

```bash
# Trouver les catégories non reconnues
curl "http://localhost:8080/analysis/values/categorie?vendor=Demarne&date_from=2026-01-26" \
  -H "X-API-Key: $KEY" | jq '.top_values[] | select(.value | contains(" "))'

# Résultat: "SAUMON SUPERIEUR NORVEGE" → à décomposer dans le parseur
```

---

## Métriques de Qualité Cibles

| Champ | Couverture Cible | Priorité |
|-------|------------------|----------|
| `categorie` | 100% | Critique |
| `origine` | >80% | Haute |
| `calibre` | >70% | Haute |
| `methode_peche` | >60% | Moyenne |
| `qualite` | >50% | Moyenne |
| `decoupe` | >40% | Basse |
| `etat` | >30% | Basse |

**Attributs spécifiques** (trim, label, conservation) ne s'appliquent qu'à certains vendors.

---

## Alertes et Monitoring

### Détecter une Régression

```sql
-- Couverture par date pour un vendor
SELECT
  date,
  COUNTIF(categorie IS NOT NULL) / COUNT(*) * 100 AS categorie_pct,
  COUNTIF(methode_peche IS NOT NULL) / COUNT(*) * 100 AS methode_peche_pct,
  COUNTIF(origine IS NOT NULL) / COUNT(*) * 100 AS origine_pct
FROM `lacriee.PROD.AllPrices`
WHERE vendor = 'VVQM'
GROUP BY date
ORDER BY date DESC;
```

Si couverture baisse significativement → bug dans parseur ou format fichier changé.

---

## Commandes Fréquentes

```bash
# Quick check tous vendors
curl "http://localhost:8080/analysis/compare-vendors?date_from=2026-01-26" \
  -H "X-API-Key: $KEY" | jq '.comparison[] | {vendor, rows: .total_rows, categorie: .coverage.categorie, methode_peche: .coverage.methode_peche}'

# Deep dive vendor spécifique
VENDOR="Demarne"
curl "http://localhost:8080/analysis/coverage?vendor=$VENDOR&date_from=2026-01-26" -H "X-API-Key: $KEY"
curl "http://localhost:8080/analysis/values/categorie?vendor=$VENDOR&date_from=2026-01-26" -H "X-API-Key: $KEY"
curl "http://localhost:8080/analysis/values/origine?vendor=$VENDOR&date_from=2026-01-26" -H "X-API-Key: $KEY"
```

---

## Troubleshooting

### Erreur 401 Unauthorized
Vérifier que `X-API-Key` header est bien défini:
```bash
export PDF_PARSER_API_KEY="valeur_depuis_.env.local"
```

### Données anciennes retournées
Toujours filtrer par date récente:
```bash
?date_from=2026-01-26
```

### Streaming buffer BigQuery
Attendre 1-2 min après un import avant d'analyser:
```bash
sleep 120
```

---

## Références

- **Mappings harmonisation**: [docs/CLAUDE.md](CLAUDE.md#harmonisation-des-attributs)
- **Schema AllPrices**: [scripts/init_db.sql](../scripts/init_db.sql)
- **Service analysis**: [services/quality_analysis.py](../services/quality_analysis.py)
