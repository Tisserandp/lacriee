-- Script de creation de la table VVQMStructured
-- Table dediee au parsing structure des fichiers VVQM
-- Separee de ProvidersPrices pour eviter les conflits
-- Projet: lacriee (utiliser lacrieeparseur.json pour les credentials)

CREATE TABLE IF NOT EXISTS `lacriee.PROD.VVQMStructured` (
    keyDate STRING NOT NULL OPTIONS(description="Cle unique: Code_Provider_Date"),
    Code STRING NOT NULL OPTIONS(description="Code produit VVQM"),
    Code_Provider STRING OPTIONS(description="Alias de Code pour compatibilite"),

    -- Attributs structures extraits du nom produit
    Espece STRING OPTIONS(description="Espece de base (ex: BAR, TURBOT, MERLU)"),
    Methode_Peche STRING OPTIONS(description="Methode de peche (ex: PB, LIGNE, IKEJIME)"),
    Etat STRING OPTIONS(description="Etat/preparation (ex: VIDE, CORAIL, BLANCHE, VIVANT)"),
    Decoupe STRING OPTIONS(description="Type de decoupe (ex: DOS, FILET, JOUE, LONGE)"),
    Origine STRING OPTIONS(description="Origine geographique (ex: ATLANTIQUE, DANEMARK)"),
    Section STRING OPTIONS(description="Section du PDF (COQUILLAGES, CRUSTACES BRETONS, FILETS)"),
    Calibre STRING OPTIONS(description="Calibre/taille (ex: 1/2, 2/3, 0)"),

    -- Prix et categorie
    Prix FLOAT64 OPTIONS(description="Prix unitaire"),
    Categorie STRING OPTIONS(description="Categorie automatique (section ou mapping espece)"),
    ProductName STRING OPTIONS(description="Nom complet: Produit - Calibre"),

    -- Metadonnees
    Date STRING NOT NULL OPTIONS(description="Date du tarif (YYYY-MM-DD)"),
    Vendor STRING NOT NULL OPTIONS(description="Fournisseur: VVQM"),

    -- Audit
    import_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP() OPTIONS(description="Date d'import")
)
PARTITION BY DATE(PARSE_DATE('%Y-%m-%d', Date))
CLUSTER BY Categorie, Espece
OPTIONS(
    description="Table structuree des tarifs VVQM avec attributs decomposes",
    labels=[("vendor", "vvqm"), ("type", "structured")]
);

-- Note: BigQuery ne supporte pas les index explicites, mais le clustering aide
-- Les requetes filtrant par Categorie ou Espece seront optimisees
