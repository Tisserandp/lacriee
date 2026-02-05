# Système de Mapping BigQuery - Guide Complet

## Vue d'ensemble

Le système de mapping permet de normaliser les catégories brutes des parseurs vers des catégories standards (`famille_std` et `espece_std`) utilisées dans les analyses.

**Architecture:**
```
AllPrices (categorie, decoupe)
    → Mapping_Categories (categorie_raw, decoupe → famille_std, espece_std)
    → Analytics_Produits_Comparaison
```

## 🚀 Quick Start: Modifier un Mapping

**Prérequis**: Toutes les commandes s'exécutent depuis la racine du projet (là où se trouve `docker-compose.yml`).

**Pour corriger ou ajouter un mapping en 3 étapes:**

1. **Créer/modifier** `scripts/update_mapping_XXX.sql` avec le CREATE OR REPLACE VIEW complet

2. **Exécuter** la mise à jour:
```bash
docker exec fastapi-pdf-parser python3 -c "
from google.cloud import bigquery
client = bigquery.Client(project='lacriee')
sql = open('scripts/update_mapping_XXX.sql', 'r', encoding='utf-8').read()
client.query(sql).result()
print('✅ Vue mise à jour')
"
```

3. **Recalculer Analytics**:
```bash
docker exec fastapi-pdf-parser python3 -c "
from google.cloud import bigquery
client = bigquery.Client(project='lacriee')
client.query('CALL \`lacriee.PROD.sp_Update_Analytics_Produits_Comparaison\`()').result()
print('✅ Analytics recalculé')
"
```

**C'est tout !** Voir sections détaillées ci-dessous pour plus d'exemples.

## 1. Vue `Mapping_Categories`

### Localisation
- **Dataset**: `lacriee.PROD`
- **Type**: VIEW
- **Utilisée par**:
  - `sp_Update_Analytics_Produits_Comparaison` (stored procedure principale)
  - `Mapping_Calibres` (pour récupérer espece_std)
  - `V_Prix_Du_Jour` (vue alternative, peu utilisée)

### Structure

```sql
CREATE OR REPLACE VIEW `lacriee.PROD.Mapping_Categories` AS
SELECT * FROM UNNEST([
  STRUCT(
    'BAR' AS categorie_raw,           -- Catégorie brute du parseur
    CAST(NULL AS STRING) AS decoupe,  -- Découpe spécifique (NULL = match générique)
    'POISSON' AS famille_std,          -- Famille normalisée
    'BAR' AS espece_std                -- Espèce normalisée
  ),
  -- ... ~150 lignes de mapping
]);
```

### Colonnes

| Colonne | Type | Description | Exemple |
|---------|------|-------------|---------|
| `categorie_raw` | STRING | Catégorie brute extraite par les parseurs | `ANCHOIS`, `SAINT PIERRE` |
| `decoupe` | STRING (nullable) | Découpe spécifique pour mapping conditionnel | `FILET`, `NULL` |
| `famille_std` | STRING | Famille standard pour regroupement | `POISSON`, `EPICERIE`, `CRUSTACE` |
| `espece_std` | STRING | Espèce standard pour analyses détaillées | `ANCHOIS`, `SAINT_PIERRE` |

### Logique de Mapping avec `decoupe`

**Principe**: La colonne `decoupe` permet de gérer des cas spéciaux où un même produit doit être classé différemment selon sa découpe.

**Exemple ANCHOIS**:
```sql
-- Anchois entiers = POISSON
STRUCT('ANCHOIS', CAST(NULL AS STRING), 'POISSON', 'ANCHOIS'),

-- Filets d'anchois marinés = EPICERIE
STRUCT('ANCHOIS', 'FILET', 'EPICERIE', 'EPICERIE'),
```

**Résultat**:
- Produit avec `categorie='ANCHOIS'` et `decoupe=NULL` → `POISSON/ANCHOIS`
- Produit avec `categorie='ANCHOIS'` et `decoupe='FILET'` → `EPICERIE/EPICERIE`

### Règles Importantes

1. **CAST(NULL AS STRING) obligatoire**: Ne jamais utiliser `NULL` seul, toujours `CAST(NULL AS STRING)` pour éviter les erreurs de type BigQuery

2. **Ordre de priorité**: Dans les jointures, les matchs spécifiques (avec `decoupe`) ont priorité sur les matchs génériques (`decoupe=NULL`)

3. **Exhaustivité**: Tout produit non matché tombe en `A CLASSER`

## 2. Procédure `sp_Update_Analytics_Produits_Comparaison`

### Localisation
- **Dataset**: `lacriee.PROD`
- **Type**: PROCEDURE
- **Fréquence d'exécution**: Après chaque import ou à la demande

### Jointure avec Mapping_Categories

```sql
-- CTE: with_categories
SELECT
  p.*,
  s.avg_prix_90j,
  s.count_prix_90j,
  COALESCE(cat.famille_std, 'A CLASSER') as famille_std,
  COALESCE(cat.espece_std, 'A CLASSER') as espece_std
FROM last_prices p
LEFT JOIN stats_90j s ON p.code_provider = s.code_provider
LEFT JOIN `lacriee.PROD.Mapping_Categories` cat
  ON TRIM(UPPER(p.categorie)) = TRIM(UPPER(cat.categorie_raw))
  AND (
    -- Match exact avec decoupe spécifique
    (cat.decoupe IS NOT NULL AND TRIM(UPPER(COALESCE(p.decoupe, ''))) = TRIM(UPPER(cat.decoupe)))
    OR
    -- Match générique (cat.decoupe = NULL)
    (cat.decoupe IS NULL)
  )
-- Priorité aux matchs spécifiques (decoupe non NULL)
QUALIFY ROW_NUMBER() OVER(
  PARTITION BY p.code_provider
  ORDER BY CASE WHEN cat.decoupe IS NOT NULL THEN 1 ELSE 2 END
) = 1
```

**Explication QUALIFY**:
- Permet de ne garder qu'un seul mapping par produit
- Priorité 1: Match avec `decoupe` non NULL (spécifique)
- Priorité 2: Match avec `decoupe` NULL (générique)

## 3. Ajouter un Nouveau Mapping

### Workflow Complet (Méthode Recommandée)

**Étape 1: Créer le fichier SQL de mise à jour**

Créer `scripts/update_mapping_XXX.sql` avec votre modification:
```sql
CREATE OR REPLACE VIEW `lacriee.PROD.Mapping_Categories` AS
SELECT * FROM UNNEST([
  -- ... copier toute la vue existante avec vos modifications
  STRUCT('POULPE', CAST(NULL AS STRING), 'CEPHALOPODE', 'POULPE'),  -- ← Votre ajout
  -- ... reste de la vue
]);
```

**Étape 2: Exécuter la mise à jour (MÉTHODE SIMPLE)**

```bash
docker exec fastapi-pdf-parser python3 -c "
from google.cloud import bigquery
client = bigquery.Client(project='lacriee')
sql = open('scripts/update_mapping_XXX.sql', 'r', encoding='utf-8').read()
client.query(sql).result()
print('✅ Vue mise à jour')
"
```

**Étape 3: Recalculer Analytics**

```bash
docker exec fastapi-pdf-parser python3 -c "
from google.cloud import bigquery
client = bigquery.Client(project='lacriee')
client.query('CALL \`lacriee.PROD.sp_Update_Analytics_Produits_Comparaison\`()').result()
print('✅ Analytics recalculé')
"
```

**Étape 4: Vérifier**

```bash
docker exec fastapi-pdf-parser python3 -c "
from google.cloud import bigquery
client = bigquery.Client(project='lacriee')
result = client.query('''
  SELECT categorie, espece_std, COUNT(*) as nb
  FROM \`lacriee.PROD.Analytics_Produits_Comparaison\`
  WHERE categorie = \"POULPE\"
  GROUP BY categorie, espece_std
''').result()
for row in result:
    print(f'{row.categorie} -> {row.espece_std}: {row.nb} produits')
"
```

### Cas Simple: Nouvelle Catégorie

**Exemple**: Ajouter "POULPE" en CEPHALOPODE

1. Récupérer la vue actuelle pour backup:
```bash
docker exec fastapi-pdf-parser python3 -c "
from google.cloud import bigquery
client = bigquery.Client(project='lacriee')
table = client.get_table('lacriee.PROD.Mapping_Categories')
print(table.view_query)
" > scripts/mapping_backup_$(date +%Y%m%d).sql
```

2. Modifier la vue et suivre le **Workflow Complet** ci-dessus

### Cas Avancé: Mapping Conditionnel par Découpe

**Exemple**: Séparer maquereaux frais (POISSON) des maquereaux fumés (EPICERIE)

1. Vérifier les données existantes:
```sql
SELECT DISTINCT categorie, decoupe, COUNT(*) as nb
FROM `lacriee.PROD.AllPrices`
WHERE categorie LIKE '%MAQUEREAU%'
GROUP BY categorie, decoupe;
```

2. Ajouter les mappings:
```sql
-- Maquereaux frais par défaut
STRUCT('MAQUEREAU', CAST(NULL AS STRING), 'POISSON', 'MAQUEREAU'),

-- Maquereaux fumés en épicerie
STRUCT('MAQUEREAU', 'FUME', 'EPICERIE', 'EPICERIE'),
```

3. **Aucune modification de la stored procedure nécessaire** si la logique de priorité existe déjà

## 4. Modifier un Mapping Existant

### Procédure Recommandée

**Suivre le Workflow Complet de la section 3** avec ces étapes:

1. **Backup** (optionnel mais recommandé):
```bash
docker exec fastapi-pdf-parser python3 -c "
from google.cloud import bigquery
client = bigquery.Client(project='lacriee')
table = client.get_table('lacriee.PROD.Mapping_Categories')
print(table.view_query)
" > scripts/mapping_backup_$(date +%Y%m%d).sql
```

2. **Créer le fichier SQL** avec votre modification (ex: `scripts/fix_dorade_grise.sql`)

3. **Exécuter**:
```bash
docker exec fastapi-pdf-parser python3 -c "
from google.cloud import bigquery
client = bigquery.Client(project='lacriee')
sql = open('scripts/fix_dorade_grise.sql', 'r', encoding='utf-8').read()
client.query(sql).result()
print('✅ Vue mise à jour')
"
```

4. **Recalculer Analytics**:
```bash
docker exec fastapi-pdf-parser python3 -c "
from google.cloud import bigquery
client = bigquery.Client(project='lacriee')
client.query('CALL \`lacriee.PROD.sp_Update_Analytics_Produits_Comparaison\`()').result()
print('✅ Analytics recalculé')
"
```

5. **Vérifier**:
```bash
docker exec fastapi-pdf-parser python3 -c "
from google.cloud import bigquery
client = bigquery.Client(project='lacriee')
result = client.query('''
  SELECT categorie, espece_std, COUNT(*) as nb
  FROM \`lacriee.PROD.Analytics_Produits_Comparaison\`
  WHERE categorie = \"DORADE GRISE\"
  GROUP BY categorie, espece_std
''').result()
for row in result:
    print(f'{row.categorie} -> {row.espece_std}: {row.nb} produits')
"
```

## 5. Familles Standards Utilisées

| Famille | Description | Exemples |
|---------|-------------|----------|
| `POISSON` | Poissons frais | BAR, SOLE, SAUMON |
| `CRUSTACE` | Crustacés | HOMARD, LANGOUSTINE, TOURTEAU |
| `COQUILLAGE` | Coquillages | HUITRE, MOULE, ST JACQUES |
| `CEPHALOPODE` | Céphalopodes | SEICHE, ENCORNET, POULPE |
| `EPICERIE` | Produits élaborés/marinés | Filets marinés, sauces, blinis |
| `TRAITEUR` | Plats cuisinés | Soupes, terrines, fumés |
| `DIVERS` | Autres produits | Grenouilles, algues |
| `SURGELE` | Produits surgelés | Céphalopodes surgelés premium |
| `A CLASSER` | Produits non mappés | (à éviter) |

## 6. Diagnostic des Problèmes de Mapping

### Trouver les produits mal classés

```sql
-- Produits en "A CLASSER"
SELECT categorie, decoupe, product_name, COUNT(*) as nb
FROM `lacriee.PROD.Analytics_Produits_Comparaison`
WHERE famille_std = 'A CLASSER'
GROUP BY categorie, decoupe, product_name
ORDER BY nb DESC;
```

### Vérifier un mapping spécifique

```sql
-- Voir comment une catégorie est mappée
SELECT *
FROM `lacriee.PROD.Mapping_Categories`
WHERE categorie_raw LIKE '%ANCHOIS%'
ORDER BY decoupe NULLS FIRST;
```

### Compter les produits par famille

```sql
SELECT famille_std, espece_std, COUNT(*) as nb_produits
FROM `lacriee.PROD.Analytics_Produits_Comparaison`
GROUP BY famille_std, espece_std
ORDER BY famille_std, nb_produits DESC;
```

## 7. Checklist de Modification

- [ ] Identifier la catégorie brute à mapper (`categorie` dans AllPrices)
- [ ] Vérifier si un mapping conditionnel est nécessaire (basé sur `decoupe`)
- [ ] Backup de la vue `Mapping_Categories` actuelle
- [ ] Ajouter/modifier les STRUCT dans la vue (avec `CAST(NULL AS STRING)`)
- [ ] Tester la requête de vue (CREATE OR REPLACE VIEW)
- [ ] Recalculer `Analytics_Produits_Comparaison`
- [ ] Vérifier le résultat avec une requête de contrôle
- [ ] Documenter le changement si c'est un cas particulier

## 8. Exemples de Mappings Conditionnels Existants

### Anchois
```sql
STRUCT('ANCHOIS', CAST(NULL AS STRING), 'POISSON', 'ANCHOIS'),     -- Anchois frais
STRUCT('ANCHOIS', 'FILET', 'EPICERIE', 'EPICERIE'),                -- Filets marinés
```

**Raison**: Les filets d'anchois marinés sont des produits d'épicerie, pas du poisson frais.

### Aide Culinaires et Blinis
```sql
STRUCT('AIDE CULINAIRES', CAST(NULL AS STRING), 'EPICERIE', 'EPICERIE'),  -- Aides culinaires
STRUCT('BLINIS', CAST(NULL AS STRING), 'EPICERIE', 'EPICERIE'),          -- Blinis
```

**Raison (modifié le 2026-02-05)**: Toutes les catégories qui étaient envoyées vers `AIDE_CULINAIRE` ou `ACCOMPAGNEMENT` sont maintenant redirigées vers l'espèce standard `EPICERIE` pour simplifier la classification.

## 9. Accès Rapide aux Vues

### Méthode Recommandée: Via Docker + Client BigQuery

**IMPORTANT**: Toutes les commandes s'exécutent depuis la racine du projet LaCriee.

#### Lire la vue actuelle (pour backup)
```bash
docker exec fastapi-pdf-parser python3 -c "
from google.cloud import bigquery
client = bigquery.Client(project='lacriee')
table = client.get_table('lacriee.PROD.Mapping_Categories')
print(table.view_query)
" > scripts/mapping_backup_$(date +%Y%m%d).sql
```

#### Exécuter un fichier SQL (MÉTHODE SIMPLE)
```bash
# Le fichier SQL doit être dans scripts/ et accessible depuis le conteneur
docker exec fastapi-pdf-parser python3 -c "
from google.cloud import bigquery
client = bigquery.Client(project='lacriee')
sql = open('scripts/mon_script.sql', 'r', encoding='utf-8').read()
client.query(sql).result()
print('✅ Requête exécutée')
"
```

#### Recalculer Analytics
```bash
docker exec fastapi-pdf-parser python3 -c "
from google.cloud import bigquery
client = bigquery.Client(project='lacriee')
client.query('CALL \`lacriee.PROD.sp_Update_Analytics_Produits_Comparaison\`()').result()
print('✅ Analytics recalculé')
"
```

#### Vérifier un mapping spécifique
```bash
docker exec fastapi-pdf-parser python3 -c "
from google.cloud import bigquery
client = bigquery.Client(project='lacriee')
result = client.query('''
  SELECT categorie_raw, espece_std
  FROM \`lacriee.PROD.Mapping_Categories\`
  WHERE categorie_raw = \"VOTRE_CATEGORIE\"
''').result()
for row in result:
    print(f'{row.categorie_raw} -> {row.espece_std}')
"
```

### Via Console BigQuery (Alternative)
https://console.cloud.google.com/bigquery?project=lacriee&ws=!1m5!1m4!4m3!1slacriee!2sPROD!3sMapping_Categories

## 10. Notes Importantes

1. **Aucun changement Python nécessaire**: Tout se passe dans BigQuery (vues et stored procedures)

2. **Impact sur les analyses**: Modifier un mapping change la classification de TOUS les produits historiques de cette catégorie

3. **Cohérence**: Garder les familles cohérentes (ne pas mélanger POISSON et EPICERIE pour une même espèce sauf cas justifié)

4. **Performance**: Les vues sont recalculées à chaque requête, mais `Analytics_Produits_Comparaison` est une table matérialisée via la stored procedure

5. **Extensibilité**: Le système supporte facilement l'ajout de nouvelles colonnes de mapping (origine, qualité, etc.) en suivant le même principe que `decoupe`
