-- Script de création de la table DemarneStructured
-- Table dédiée au parsing structuré des fichiers Demarne
-- Séparée de ProvidersPrices pour éviter les conflits
-- Projet: lacriee (utiliser lacrieeparseur.json pour les credentials)

CREATE TABLE IF NOT EXISTS `lacriee.PROD.DemarneStructured` (
    keyDate STRING NOT NULL OPTIONS(description="Clé unique: Code_Date"),
    Code STRING NOT NULL OPTIONS(description="Code produit Demarne"),
    Code_Provider STRING OPTIONS(description="Alias de Code pour compatibilité"),

    -- Informations structurées du produit
    Categorie STRING OPTIONS(description="Catégorie principale FR (ex: SAUMON SUPÉRIEUR NORVÈGE)"),
    Categorie_EN STRING OPTIONS(description="Catégorie principale EN (ex: SUPERIOR NORWEGIAN SALMON)"),
    Variante STRING OPTIONS(description="Variante/préparation FR (ex: Entier, Filet, Ligne)"),
    Variante_EN STRING OPTIONS(description="Variante/préparation EN (ex: Whole, Fillet, Line)"),
    Methode_Peche STRING OPTIONS(description="Méthode de pêche extraite (ex: LIGNE, PB, IKEJIME, CASIER)"),
    Label STRING OPTIONS(description="Label/certification (ex: MSC, BIO, Trim B)"),
    Calibre STRING OPTIONS(description="Calibre/taille (ex: 1/2, 300/400, N°2)"),
    Origine STRING OPTIONS(description="Origine géographique ou zone FAO"),
    Colisage STRING OPTIONS(description="Conditionnement (ex: 20 kg ou Pi, 3 kg)"),

    -- Prix et facturation
    Tarif FLOAT64 OPTIONS(description="Prix unitaire"),
    Prix FLOAT64 OPTIONS(description="Alias de Tarif pour compatibilité"),
    Unite_Facturee STRING OPTIONS(description="Unité de facturation (ex: Kg, U)"),

    -- Nom complet reconstitué
    ProductName STRING OPTIONS(description="Nom complet: Categorie - Variante - Label - Calibre"),

    -- Métadonnées
    Date STRING NOT NULL OPTIONS(description="Date du tarif (YYYY-MM-DD)"),
    Vendor STRING NOT NULL OPTIONS(description="Fournisseur: Demarne"),

    -- Audit
    import_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP() OPTIONS(description="Date d'import")
)
PARTITION BY DATE(PARSE_DATE('%Y-%m-%d', Date))
CLUSTER BY Categorie, Code
OPTIONS(
    description="Table structurée des tarifs Demarne avec colonnes détaillées",
    labels=[("vendor", "demarne"), ("type", "structured")]
);

-- Index pour recherches fréquentes
-- Note: BigQuery ne supporte pas les index explicites, mais le clustering aide
