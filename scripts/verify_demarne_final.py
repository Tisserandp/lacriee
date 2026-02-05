#!/usr/bin/env python3
"""Vérifier les données Demarne finales."""

from google.cloud import bigquery

client = bigquery.Client(project='lacriee')

# 1. Vérifier le vendor
query1 = """
SELECT DISTINCT vendor, COUNT(*) as count
FROM `lacriee.PROD.AllPrices`
WHERE LOWER(vendor) LIKE '%demarne%'
GROUP BY vendor
"""

print('1. VENDOR:')
print('=' * 60)
for row in client.query(query1).result():
    print(f'  ✓ {row.vendor}: {row.count} lignes')

# 2. Vérifier les états de préparation dans decoupe
# Utiliser last_job_id pour cibler le dernier chargement
query2 = """
SELECT product_name, decoupe
FROM `lacriee.PROD.AllPrices`
WHERE vendor = 'Demarne'
  AND last_job_id = '0ce3c5fe-cf76-4209-887f-bcc76407e6e7'
  AND (product_name LIKE '%vid%' OR product_name LIKE '%Entier%')
ORDER BY product_name
LIMIT 12
"""

print('\n2. ÉTATS DE PRÉPARATION (job 0ce3c5fe):')
print('=' * 80)
count = 0
for row in client.query(query2).result():
    count += 1
    decoupe_val = row.decoupe if row.decoupe else 'null'
    print(f'  {row.product_name[:52]:<52} | decoupe: {decoupe_val}')

if count == 0:
    print('  ⚠ Aucune donnée trouvée (streaming buffer actif, réessayez dans 1-2 min)')

print('\n✓ Vérification terminée')
