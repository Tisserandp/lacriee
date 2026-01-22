"""
Tests de validation de l'harmonisation des attributs.
Exécute les parseurs et vérifie que la normalisation fonctionne correctement.
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from services.harmonize import (
    harmonize_product,
    harmonize_products,
    normalize_categorie,
    normalize_methode_peche,
    normalize_etat,
    normalize_origine,
    normalize_qualite,
    normalize_calibre,
    normalize_demarne_categorie,
    normalize_demarne_variante,
    normalize_demarne_label,
    clean_demarne_origine,
)


def test_categorie_mapping():
    """Test des mappings de catégorie."""
    print("\n=== Test Categorie ===")

    tests = [
        ("ST PIERRE", None, "SAINT PIERRE"),
        ("SAUMONS", None, "SAUMON"),
        ("LIEU", None, "LIEU JAUNE"),
        ("PLIE/ CARRELET", None, "CARRELET"),
        ("CRUSTACES BRETONS", None, "CRUSTACES"),
        ("BAR", None, "BAR"),  # Pas de changement
        # Test extraction FILET
        ("FILET DE POISSONS", "FILET DE BAR", "BAR"),
        ("BAR FILET", "BAR LIGNE 1/2", "BAR"),
    ]

    for categorie, product_name, expected in tests:
        result = normalize_categorie(categorie, product_name)
        actual = result["categorie"]
        status = "✓" if actual == expected else "✗"
        print(f"  {status} '{categorie}' → '{actual}' (attendu: '{expected}')")

    # Test extraction decoupe depuis categorie FILET
    result = normalize_categorie("FILET DE POISSONS", "FILET DE BAR")
    assert result["decoupe_from_categorie"] == "FILET", "Decoupe devrait être extrait"
    print(f"  ✓ 'FILET DE POISSONS' → decoupe='FILET' extrait")


def test_methode_peche_mapping():
    """Test des mappings de méthode de pêche."""
    print("\n=== Test Methode_Peche ===")

    tests = [
        ("PT BATEAU", {"methode_peche": "PB", "type_production": None, "technique_abattage": None}),
        ("PETIT BATEAU", {"methode_peche": "PB", "type_production": None, "technique_abattage": None}),
        ("LIGNE", {"methode_peche": "LIGNE", "type_production": None, "technique_abattage": None}),
        ("LIGNE IKEJIME", {"methode_peche": "LIGNE", "type_production": None, "technique_abattage": "IKEJIME"}),
        ("SAUVAGE", {"methode_peche": None, "type_production": "SAUVAGE", "technique_abattage": None}),
        ("CHALUT", {"methode_peche": "CHALUT", "type_production": None, "technique_abattage": None}),
    ]

    for methode, expected in tests:
        result = normalize_methode_peche(methode)
        match = all(result.get(k) == v for k, v in expected.items())
        status = "✓" if match else "✗"
        print(f"  {status} '{methode}' → {result}")
        if not match:
            print(f"      Attendu: {expected}")


def test_etat_mapping():
    """Test des mappings d'état."""
    print("\n=== Test Etat ===")

    tests = [
        ("VIDEE", {"etat": "VIDE", "couleur": None}),
        ("VIDÉ", {"etat": "VIDE", "couleur": None}),
        ("CORAILLEES", {"etat": "CORAILLE", "couleur": None}),
        ("CORAIL", {"etat": "CORAILLE", "couleur": None}),
        ("PELEE", {"etat": "PELE", "couleur": None}),
        ("ENTIÈRE", {"etat": "ENTIER", "couleur": None}),
        # Couleurs → champ dédié
        ("ROUGE", {"etat": None, "couleur": "ROUGE"}),
        ("BLANCHE", {"etat": None, "couleur": "BLANCHE"}),
        ("NOIRE", {"etat": None, "couleur": "NOIRE"}),
    ]

    for etat, expected in tests:
        result = normalize_etat(etat)
        match = all(result.get(k) == v for k, v in expected.items())
        status = "✓" if match else "✗"
        print(f"  {status} '{etat}' → {result}")
        if not match:
            print(f"      Attendu: {expected}")


def test_origine_mapping():
    """Test des mappings d'origine."""
    print("\n=== Test Origine ===")

    tests = [
        ("BRETON", {"origine": "BRETAGNE", "type_production": None}),
        ("VAT", {"origine": "ATLANTIQUE", "type_production": None}),
        ("VDK", {"origine": "DANEMARK", "type_production": None}),
        ("ECOSSE", {"origine": "ECOSSE", "type_production": None}),
        ("AQUACULTURE", {"origine": None, "type_production": "ELEVAGE"}),
        ("FAO27", {"origine": "FAO27", "type_production": None}),
        # Multi-origines
        ("FRANCE, ECOSSE", {"origine": "FRANCE, ECOSSE", "type_production": None}),
    ]

    for origine, expected in tests:
        result = normalize_origine(origine)
        match = all(result.get(k) == v for k, v in expected.items())
        status = "✓" if match else "✗"
        print(f"  {status} '{origine}' → {result}")
        if not match:
            print(f"      Attendu: {expected}")


def test_qualite_mapping():
    """Test des mappings de qualité."""
    print("\n=== Test Qualite ===")

    tests = [
        ("QUALITE PREMIUM", "PREMIUM"),
        ("EXTRA", "EXTRA"),
        ("EXTRA PINS", "EXTRA PINS"),
        ("SUP", "SUP"),
        ("XX", "XX"),
    ]

    for qualite, expected in tests:
        result = normalize_qualite(qualite)
        status = "✓" if result == expected else "✗"
        print(f"  {status} '{qualite}' → '{result}' (attendu: '{expected}')")


def test_calibre_normalization():
    """Test de la normalisation des calibres."""
    print("\n=== Test Calibre ===")

    tests = [
        ("1,5/2", "1.5/2"),
        ("0,8/1,3", "0.8/1.3"),
        ("500/+", "500+"),
        ("1/+", "1+"),
        ("+2", "2+"),
        ("T2", "T2"),
        ("N°3", "N°3"),
        ("JUMBO", "JUMBO"),
        ("2/3", "2/3"),  # Pas de changement
    ]

    for calibre, expected in tests:
        result = normalize_calibre(calibre)
        status = "✓" if result == expected else "✗"
        print(f"  {status} '{calibre}' → '{result}' (attendu: '{expected}')")


def test_full_product_harmonization():
    """Test d'harmonisation complète d'un produit."""
    print("\n=== Test Harmonisation Produit Complet ===")

    # Produit Hennequin avec plusieurs champs à normaliser
    product_hennequin = {
        "Categorie": "ST PIERRE",
        "Methode_Peche": "PT BATEAU",
        "Qualite": "QUALITE PREMIUM",
        "Etat": "VIDEE",
        "Origine": "ECOSSE",
        "Calibre": "1,5/2",
        "Conservation": "CONGELEE",
        "ProductName": "ST PIERRE PT BATEAU 1.5/2",
    }

    result = harmonize_product(product_hennequin, vendor="Hennequin")

    print(f"  Input: {product_hennequin}")
    print(f"  Output: {result}")

    checks = [
        ("categorie", "SAINT PIERRE"),
        ("methode_peche", "PB"),
        ("qualite", "PREMIUM"),
        ("etat", "VIDE"),
        ("origine", "ECOSSE"),
        ("calibre", "1.5/2"),
        ("conservation", "CONGELE"),
    ]

    for field, expected in checks:
        actual = result.get(field)
        status = "✓" if actual == expected else "✗"
        print(f"    {status} {field}: '{actual}' (attendu: '{expected}')")


def test_demarne_categorie_mapping():
    """Test des mappings de catégorie Demarne."""
    print("\n=== Test Demarne Categorie ===")

    tests = [
        ("SAUMON SUPÉRIEUR NORVÈGE", {"categorie": "SAUMON", "qualite": "SUP", "origine_from_categorie": "NORVEGE"}),
        ("BAR SAUVAGE", {"categorie": "BAR", "type_production": "SAUVAGE"}),
        ("BAR ÉLEVAGE ENTIER", {"categorie": "BAR", "type_production": "ELEVAGE", "etat": "ENTIER"}),
        ("DORADE ÉLEVAGE VIDÉ GRATTÉ", {"categorie": "DORADE", "type_production": "ELEVAGE", "etat": "VIDE"}),
        ("CREVETTE SAUVAGE CUITE", {"categorie": "CREVETTES", "type_production": "SAUVAGE", "etat": "CUIT"}),
        ("HOMARD CANADIEN", {"categorie": "HOMARD", "origine_from_categorie": "CANADA"}),
        ("HOMARD EUROPEEN", {"categorie": "HOMARD", "origine_from_categorie": "EUROPE"}),
        ("LA BELON", {"categorie": "HUITRES"}),
        ("LA PERLE NOIRE", {"categorie": "HUITRES"}),
        ("HUITRE DE NORMANDIE", {"categorie": "HUITRES"}),
        ("FILETS POISSON BLANC", {"categorie": "FILET"}),
        ("SAUMON ÉCOSSE LABEL ROUGE", {"categorie": "SAUMON", "qualite": "LABEL ROUGE", "origine_from_categorie": "ECOSSE"}),
    ]

    for categorie, expected in tests:
        result = normalize_demarne_categorie(categorie)
        # Vérifier les champs attendus
        match = all(result.get(k) == v for k, v in expected.items())
        status = "✓" if match else "✗"
        print(f"  {status} '{categorie}' → {result}")
        if not match:
            print(f"      Attendu: {expected}")


def test_demarne_variante_mapping():
    """Test des mappings de variante Demarne."""
    print("\n=== Test Demarne Variante ===")

    tests = [
        ("Filet", {"decoupe": "FILET"}),
        ("Dos de cabillaud", {"decoupe": "DOS"}),
        ("Queue de lotte", {"decoupe": "QUEUE"}),
        ("Pavé de saumon", {"decoupe": "PAVE"}),
        ("Entier", {"etat": "ENTIER"}),
        ("Vivant", {"etat": "VIVANT"}),
        ("Cuite fraiche", {"etat": "CUIT"}),
        ("Filet de merlu", {"decoupe": "FILET"}),
    ]

    for variante, expected in tests:
        result = normalize_demarne_variante(variante)
        match = all(result.get(k) == v for k, v in expected.items())
        status = "✓" if match else "✗"
        print(f"  {status} '{variante}' → {result}")
        if not match:
            print(f"      Attendu: {expected}")


def test_demarne_label_mapping():
    """Test des mappings de label Demarne."""
    print("\n=== Test Demarne Label ===")

    tests = [
        ("MSC", {"label": "MSC", "trim": None}),
        ("BIO", {"label": "BIO", "trim": None}),
        ("Trim B", {"label": None, "trim": "TRIM_B"}),
        ("Trim D", {"label": None, "trim": "TRIM_D"}),
        ("MSC Label Rouge", {"label": "MSC, LABEL ROUGE", "trim": None}),
        ("ASC", {"label": "ASC", "trim": None}),
    ]

    for label, expected in tests:
        result = normalize_demarne_label(label)
        match = all(result.get(k) == v for k, v in expected.items())
        status = "✓" if match else "✗"
        print(f"  {status} '{label}' → {result}")
        if not match:
            print(f"      Attendu: {expected}")


def test_demarne_origine_cleaning():
    """Test du nettoyage des origines Demarne."""
    print("\n=== Test Demarne Origine Cleaning ===")

    tests = [
        ("France", "FRANCE"),
        ("Norvège", "NORVEGE"),
        ("Écosse", "ECOSSE"),
        ("Dannemark", "DANEMARK"),
        # Poids à filtrer
        ("200 grs", None),
        ("1 kg", None),
        ("780 grs", None),
        # Codes
        ("FAO27", "FAO27"),
        ("UK - DK", "ROYAUME-UNI, DANEMARK"),
    ]

    for origine, expected in tests:
        result = clean_demarne_origine(origine)
        status = "✓" if result == expected else "✗"
        print(f"  {status} '{origine}' → '{result}' (attendu: '{expected}')")


def test_demarne_full_product_harmonization():
    """Test d'harmonisation complète d'un produit Demarne."""
    print("\n=== Test Harmonisation Produit Demarne Complet ===")

    product_demarne = {
        "Categorie": "SAUMON SUPÉRIEUR NORVÈGE",
        "Variante": "Filet",
        "Label": "MSC",
        "Calibre": "2/3",
        "Origine": "Norvège",
        "Methode_Peche": "LIGNE",
        "ProductName": "SAUMON SUPÉRIEUR NORVÈGE - Filet - MSC - 2/3",
    }

    result = harmonize_product(product_demarne, vendor="Demarne")

    print(f"  Input: {product_demarne}")
    print(f"  Output: {result}")

    checks = [
        ("categorie", "SAUMON"),
        ("qualite", "SUP"),
        ("decoupe", "FILET"),
        ("label", "MSC"),
        ("calibre", "2/3"),
        ("origine", "NORVEGE"),
        ("methode_peche", "LIGNE"),
    ]

    for field, expected in checks:
        actual = result.get(field)
        status = "✓" if actual == expected else "✗"
        print(f"    {status} {field}: '{actual}' (attendu: '{expected}')")


def test_with_real_parsers():
    """Test avec les vrais parseurs sur les fichiers d'exemple."""
    print("\n=== Test avec Parseurs Réels (via modules parsers/) ===")

    # Test Audierne via le nouveau module
    try:
        from parsers import audierne

        with open("Samples/Audierne/cours_20250930_GMS-1.60.pdf", "rb") as f:
            file_bytes = f.read()

        # Test sans harmonisation
        data_raw = audierne.parse(file_bytes, harmonize=False)
        # Test avec harmonisation
        data_harmonized = audierne.parse(file_bytes, harmonize=True)

        print(f"\n  Audierne: {len(data_raw)} produits")
        print(f"    - Sans harmonisation: clés = {list(data_raw[0].keys())[:5]}...")
        print(f"    - Avec harmonisation: clés = {list(data_harmonized[0].keys())[:5]}...")

        st_pierre_count = sum(1 for p in data_harmonized if p.get("categorie") == "SAINT PIERRE")
        print(f"    - SAINT PIERRE: {st_pierre_count} produits")

        trim_count = sum(1 for p in data_harmonized if p.get("trim") and p["trim"].startswith("TRIM_"))
        print(f"    - TRIM normalisés: {trim_count} produits")

    except Exception as e:
        print(f"  ⚠ Audierne: Erreur - {e}")

    # Test Hennequin via le nouveau module
    try:
        from parsers import hennequin

        with open("Samples/Hennequin/cours_20260114_COURS HENN.pdf", "rb") as f:
            file_bytes = f.read()

        data_harmonized = hennequin.parse(file_bytes, harmonize=True)

        print(f"\n  Hennequin: {len(data_harmonized)} produits")

        pb_count = sum(1 for p in data_harmonized if p.get("methode_peche") == "PB")
        print(f"    - PB (normalisé depuis PT BATEAU): {pb_count} produits")

        sauvage_count = sum(1 for p in data_harmonized if p.get("type_production") == "SAUVAGE")
        print(f"    - type_production=SAUVAGE: {sauvage_count} produits")

    except Exception as e:
        print(f"  ⚠ Hennequin: Erreur - {e}")

    # Test VVQM via le nouveau module
    try:
        from parsers import vvqm

        with open("Samples/VVQ/GEXPORT.pdf", "rb") as f:
            file_bytes = f.read()

        data_harmonized = vvqm.parse(file_bytes, harmonize=True)

        print(f"\n  VVQM: {len(data_harmonized)} produits")

        ikejime_count = sum(1 for p in data_harmonized if p.get("technique_abattage") == "IKEJIME")
        print(f"    - technique_abattage=IKEJIME: {ikejime_count} produits")

    except Exception as e:
        print(f"  ⚠ VVQM: Erreur - {e}")

    # Test Laurent Daniel via le nouveau module
    try:
        from parsers import laurent_daniel

        with open("Samples/LaurentD/CC2.pdf", "rb") as f:
            file_bytes = f.read()

        data_harmonized = laurent_daniel.parse(file_bytes, harmonize=True)

        print(f"\n  Laurent Daniel: {len(data_harmonized)} produits")

        couleur_count = sum(1 for p in data_harmonized if p.get("couleur"))
        print(f"    - couleur extraite: {couleur_count} produits")

        bretagne_count = sum(1 for p in data_harmonized if p.get("origine") == "BRETAGNE")
        print(f"    - origine=BRETAGNE (normalisé depuis BRETON): {bretagne_count} produits")

    except Exception as e:
        print(f"  ⚠ Laurent Daniel: Erreur - {e}")

    # Test Demarne via le nouveau module
    try:
        from parsers import demarne

        data_harmonized = demarne.parse(
            "Samples/Demarne/Classeur1 G19.xlsx",
            harmonize=True,
            date_fallback="2026-01-15"
        )

        print(f"\n  Demarne: {len(data_harmonized)} produits")

        # Statistiques des catégories harmonisées
        saumon_count = sum(1 for p in data_harmonized if p.get("categorie") == "SAUMON")
        print(f"    - categorie=SAUMON: {saumon_count} produits")

        huitres_count = sum(1 for p in data_harmonized if p.get("categorie") == "HUITRES")
        print(f"    - categorie=HUITRES: {huitres_count} produits")

        # Type production
        sauvage_count = sum(1 for p in data_harmonized if p.get("type_production") == "SAUVAGE")
        elevage_count = sum(1 for p in data_harmonized if p.get("type_production") == "ELEVAGE")
        print(f"    - type_production=SAUVAGE: {sauvage_count} produits")
        print(f"    - type_production=ELEVAGE: {elevage_count} produits")

        # Labels
        msc_count = sum(1 for p in data_harmonized if p.get("label") and "MSC" in p.get("label", ""))
        print(f"    - label contient MSC: {msc_count} produits")

        # Découpes
        filet_count = sum(1 for p in data_harmonized if p.get("decoupe") == "FILET")
        print(f"    - decoupe=FILET: {filet_count} produits")

        # Origines nettoyées (pas de poids)
        origine_valide = sum(1 for p in data_harmonized if p.get("origine") and "GRS" not in p.get("origine", "").upper())
        print(f"    - origines valides (sans poids): {origine_valide} produits")

    except Exception as e:
        print(f"  ⚠ Demarne: Erreur - {e}")


def show_sample_harmonized_products():
    """Affiche quelques produits harmonisés pour vérification visuelle."""
    print("\n=== Exemples de Produits Harmonisés ===")

    try:
        from parsers import audierne

        with open("Samples/Audierne/cours_20250930_GMS-1.60.pdf", "rb") as f:
            data = audierne.parse(f.read(), harmonize=True)

        print("\nAudierne (5 premiers produits harmonisés):")
        for i, p in enumerate(data[:5], 1):
            print(f"\n  Produit {i}:")
            for key in ["ProductName", "categorie", "methode_peche", "qualite",
                        "decoupe", "etat", "origine", "calibre", "trim", "type_production"]:
                val = p.get(key)
                if val:
                    print(f"    {key}: {val}")

    except Exception as e:
        print(f"  Erreur: {e}")


if __name__ == "__main__":
    print("=" * 70)
    print("TESTS D'HARMONISATION DES ATTRIBUTS")
    print("=" * 70)

    # Tests unitaires généraux
    test_categorie_mapping()
    test_methode_peche_mapping()
    test_etat_mapping()
    test_origine_mapping()
    test_qualite_mapping()
    test_calibre_normalization()
    test_full_product_harmonization()

    # Tests unitaires Demarne
    test_demarne_categorie_mapping()
    test_demarne_variante_mapping()
    test_demarne_label_mapping()
    test_demarne_origine_cleaning()
    test_demarne_full_product_harmonization()

    # Tests avec vrais parseurs
    test_with_real_parsers()

    # Exemples visuels
    show_sample_harmonized_products()

    print("\n" + "=" * 70)
    print("TESTS TERMINÉS")
    print("=" * 70)
