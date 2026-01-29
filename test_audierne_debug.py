#!/usr/bin/env python3
"""
Script de debug pour le parsing Audierne - vérifie step-by-step le pipeline.
"""
import json
from pathlib import Path

# Import des modules
from parsers import audierne
from services.harmonize import harmonize_products

# Charger le fichier PDF de test
test_file = Path("Samples/Audierne/cours_20260127_GMS-2.pdf")
if not test_file.exists():
    print(f"❌ Fichier non trouvé: {test_file}")
    exit(1)

print(f"✅ Fichier trouvé: {test_file}")
print(f"   Taille: {test_file.stat().st_size} bytes\n")

# Lire le contenu du fichier
with open(test_file, "rb") as f:
    file_bytes = f.read()

# ============================================================================
# ÉTAPE 1: Parse SANS harmonisation
# ============================================================================
print("=" * 80)
print("ÉTAPE 1: Parse SANS harmonisation (harmonize=False)")
print("=" * 80)

raw_data = audierne.parse(file_bytes, harmonize=False)
print(f"\n✅ Données extraites: {len(raw_data)} produits\n")

if raw_data:
    # Afficher les 3 premiers produits
    print("📋 Premiers produits (raw):")
    for i, product in enumerate(raw_data[:3]):
        print(f"\n  Product #{i+1}:")
        print(f"    Categorie: {product.get('Categorie')}")
        print(f"    ProductName: {product.get('ProductName')}")
        print(f"    Code_Provider: {product.get('Code_Provider')}")
        print(f"    Prix: {product.get('Prix')}")

    # Analyser les catégories génériques
    print("\n\n📊 Analyse des catégories génériques:")
    generic_categories = audierne.AUDIERNE_GENERIC_CATEGORIES
    print(f"   Catégories génériques à affiner: {generic_categories}\n")

    category_counts = {}
    generic_found = {}

    for product in raw_data:
        cat = product.get('Categorie')
        if cat:
            category_counts[cat] = category_counts.get(cat, 0) + 1
            if cat.upper().strip() in generic_categories:
                generic_found[cat] = generic_found.get(cat, 0) + 1

    print(f"   Total catégories trouvées: {len(category_counts)}")
    print(f"   Dont génériques: {len(generic_found)}")
    print(f"\n   Catégories génériques trouvées:")
    for cat, count in sorted(generic_found.items(), key=lambda x: -x[1]):
        print(f"      {cat}: {count} produits")

# ============================================================================
# ÉTAPE 2: Parse AVEC harmonisation
# ============================================================================
print("\n" + "=" * 80)
print("ÉTAPE 2: Parse AVEC harmonisation (harmonize=True)")
print("=" * 80)

harmonized_data = audierne.parse(file_bytes, harmonize=True)
print(f"\n✅ Données harmonisées: {len(harmonized_data)} produits\n")

if harmonized_data:
    # Afficher les 3 premiers produits
    print("📋 Premiers produits (harmonized):")
    for i, product in enumerate(harmonized_data[:3]):
        print(f"\n  Product #{i+1}:")
        print(f"    categorie: {product.get('categorie')}")
        print(f"    product_name: {product.get('product_name')}")
        print(f"    code_provider: {product.get('code_provider')}")
        print(f"    prix: {product.get('prix')}")

# ============================================================================
# ÉTAPE 3: Comparaison Raw vs Harmonized
# ============================================================================
print("\n" + "=" * 80)
print("ÉTAPE 3: Comparaison RAW vs HARMONIZED (affinage des catégories)")
print("=" * 80)

print("\n📊 Analyse de l'affinage des catégories:\n")

# Créer un mapping pour comparer
raw_map = {p.get('Code_Provider'): p.get('Categorie') for p in raw_data}
harmonized_map = {p.get('code_provider'): p.get('categorie') for p in harmonized_data}

# Catégories génériques affinées
refined_count = 0
samples_refined = []

for code, raw_cat in raw_map.items():
    if not code or code not in harmonized_map:
        continue
    harmonized_cat = harmonized_map.get(code)

    # Vérifier si c'est une catégorie générique
    if raw_cat and raw_cat.upper().strip() in audierne.AUDIERNE_GENERIC_CATEGORIES:
        if raw_cat != harmonized_cat:
            refined_count += 1
            if len(samples_refined) < 5:
                samples_refined.append({
                    'raw': raw_cat,
                    'harmonized': harmonized_cat,
                    'product_name': raw_map.get(code),
                    'code': code
                })

print(f"   ✅ Catégories génériques affinées: {refined_count}\n")
print(f"   Exemples:")
for sample in samples_refined:
    print(f"      {sample['raw']} → {sample['harmonized']}")
    # Trouver le product_name correspondant
    for prod in raw_data:
        if prod.get('Code_Provider') == sample['code']:
            print(f"        (exemple: {prod.get('ProductName')})")
            break

# ============================================================================
# ÉTAPE 4: Vérification des catégories restantes
# ============================================================================
print("\n" + "=" * 80)
print("ÉTAPE 4: Catégories restantes (génériques non affinées)")
print("=" * 80)

remaining_generics = {}
for product in harmonized_data:
    cat = product.get('categorie')
    if cat and cat.upper().strip() in audierne.AUDIERNE_GENERIC_CATEGORIES:
        remaining_generics[cat] = remaining_generics.get(cat, 0) + 1

if remaining_generics:
    print(f"\n⚠️  {len(remaining_generics)} catégories génériques restent:")
    for cat, count in sorted(remaining_generics.items(), key=lambda x: -x[1]):
        print(f"      {cat}: {count} produits")
        # Afficher un exemple
        for prod in harmonized_data:
            if prod.get('categorie') == cat:
                print(f"        → {prod.get('product_name')}")
                break
else:
    print("\n✅ Aucune catégorie générique restante!")

# ============================================================================
# ÉTAPE 5: Sauvegarder un sample
# ============================================================================
print("\n" + "=" * 80)
print("ÉTAPE 5: Sauvegarder les données pour inspection")
print("=" * 80)

sample_file = Path("test_audierne_sample.json")
sample_data = {
    "raw_count": len(raw_data),
    "harmonized_count": len(harmonized_data),
    "raw_sample": raw_data[:3] if raw_data else [],
    "harmonized_sample": harmonized_data[:3] if harmonized_data else [],
}

with open(sample_file, "w", encoding="utf-8") as f:
    json.dump(sample_data, f, indent=2, ensure_ascii=False)

print(f"\n✅ Sample sauvegardé dans: {sample_file}")

print("\n" + "=" * 80)
print("FIN DU DEBUG")
print("=" * 80)
