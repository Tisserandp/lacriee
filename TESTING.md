# Procédures de Test - LaCriee

## Tests Rapides

```bash
# Test complet (tous les vendors) - recommandé
docker exec fastapi-pdf-parser python tests/test_all_samples.py

# Test d'intégration (Laurent-Daniel seul)
docker exec fastapi-pdf-parser python tests/test_direct.py

# Debug parsers individuels
docker exec fastapi-pdf-parser python tests/test_vvqm_debug.py
docker exec fastapi-pdf-parser python tests/test_demarne_debug.py
```

## Résultats Attendus

| Vendor | Fichier | Lignes |
|--------|---------|--------|
| Laurent-Daniel | CC.pdf | 96 |
| VVQM | GEXPORT.pdf | 89 |
| Demarne | Classeur1 G19.xlsx | 691 -> 679 |
| Hennequin | cours_*.pdf | 103 |
| Audierne | cours_*.pdf | 174 |

## Validation BigQuery

### Jobs créés
```sql
SELECT job_id, vendor, filename, status, rows_extracted, rows_inserted_prod
FROM `lacriee.PROD.ImportJobs`
ORDER BY created_at DESC LIMIT 10;
```

### Données en production
```sql
SELECT vendor, COUNT(*) AS total
FROM `lacriee.PROD.AllPrices`
GROUP BY vendor;
```

### Produits non mappés
```sql
SELECT vendor, code, raw_name, occurrence_count
FROM `lacriee.PROD.UnknownProducts`
ORDER BY occurrence_count DESC LIMIT 20;
```

## Troubleshooting

### "Streaming buffer rows cannot be modified"
Normal. Attendre 1-2 min et vérifier manuellement:
```sql
SELECT COUNT(*) FROM `lacriee.PROD.AllPrices` WHERE last_job_id = 'JOB_ID';
```

### "No module named 'main'"
```bash
docker exec fastapi-pdf-parser pwd  # Doit afficher /app
```

### Parser error
```bash
docker logs fastapi-pdf-parser | grep "ERROR"
```

### Vérifier credentials BigQuery
```bash
docker exec fastapi-pdf-parser python -c "
from services.bigquery import get_bigquery_client
client = get_bigquery_client()
print(f'Project: {client.project}')
"
```

## Checklist Avant Release

- [ ] `docker exec fastapi-pdf-parser python tests/test_all_samples.py` -> tous OK
- [ ] Jobs visibles dans ImportJobs
- [ ] Données dans AllPrices
- [ ] Pas d'erreurs dans les logs
