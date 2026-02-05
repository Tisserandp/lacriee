#!/usr/bin/env python3
"""Vérifier les données Demarne chargées."""

from google.cloud import bigquery

client = bigquery.Client(project='lacriee')

# 1. Vérifier les vendors distincts
query1 = """
SELECT DISTINCT vendor, COUNT(*) as count
FROM `lacriee.PROD.AllPrices`
WHERE LOWER(vendor) LIKE '%demarne%'
GROUP BY vendor
ORDER BY vendor
"""

print('1. VENDORS DEMARNE:')
print('=' * 60)
for row in client.query(query1).result():
    print(f'  {row.vendor}: {row.count} lignes')

# 2. Vérifier les états de préparation dans decoupe
query2 = """
SELECT product_name, decoupe
FROM `lacriee.PROD.Analytics_Produits_Comparaison`
WHERE vendor = 'Demarne'
  AND (product_name LIKE '%vid%' OR product_name LIKE '%Entier%')
  AND DATE(date) = '2026-02-02'
ORDER BY product_name
LIMIT 10
"""

print('\n2. ÉTATS DE PRÉPARATION (échantillon date 2026-02-02):')
print('=' * 60)
for row in client.query(query2).result():
    print(f'{row.product_name[:50]:<50} | decoupe: {row.decoupe}')

print('\n✓ Vérification terminée')
