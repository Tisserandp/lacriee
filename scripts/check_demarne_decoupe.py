#!/usr/bin/env python3
"""Vérifier les découpes Demarne dans AllPrices."""

from google.cloud import bigquery

client = bigquery.Client(project='lacriee')

# Vérifier directement dans AllPrices (pas de délai streaming pour les nouvelles lignes)
query = """
SELECT product_name, decoupe, DATE(created_at) as created_date
FROM `lacriee.PROD.AllPrices`
WHERE vendor = 'Demarne'
  AND (product_name LIKE '%vid%' OR product_name LIKE '%Entier%')
  AND DATE(created_at) = CURRENT_DATE()
ORDER BY product_name
LIMIT 15
"""

print('ÉTATS DE PRÉPARATION dans AllPrices (chargement aujourd\'hui):')
print('=' * 80)
for row in client.query(query).result():
    print(f'{row.product_name[:55]:<55} | decoupe: {row.decoupe}')

print('\n✓ Vérification terminée')
