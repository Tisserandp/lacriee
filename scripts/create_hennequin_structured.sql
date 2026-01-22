-- Script de création de la table HennequinStructured
-- Table dédiée au parsing structuré des fichiers Hennequin
-- Séparée de ProvidersPrices pour éviter les conflits
-- Projet: lacriee (utiliser lacrieeparseur.json pour les credentials)

CREATE TABLE IF NOT EXISTS `lacriee.PROD.HennequinStructured` (
    keyDate STRING OPTIONS(description="Clé unique: Code_Provider_Date"),
    Code_Provider STRING OPTIONS(description="Code produit Hennequin"),
    
    -- Informations de base
    ProductName STRING OPTIONS(description="Nom complet du produit"),
    Categorie STRING OPTIONS(description="Catégorie du produit (ex: BAR, CABILLAUD)"),
    
    -- Attributs structurés extraits
    Methode_Peche STRING OPTIONS(description="Méthode de pêche (ex: LIGNE, PT BATEAU, SENNEUR, SAUVAGE)"),
    Qualite STRING OPTIONS(description="Qualité (ex: EXTRA, EXTRA PINS, SUP, QUALITE PREMIUM)"),
    Decoupe STRING OPTIONS(description="Type de découpe (ex: FILET, QUEUE, AILE, LONGE, PINCE, CUISSES)"),
    Etat STRING OPTIONS(description="État/préparation (ex: VIDEE, PELEE, CORAILLEES, VIVANT, CUIT)"),
    Conservation STRING OPTIONS(description="Conservation (ex: FRAIS, SURGELEE, CONGELEES, IQF)"),
    Origine STRING OPTIONS(description="Origine géographique (pays, régions, zones FAO)"),
    Calibre STRING OPTIONS(description="Calibre/taille (ex: 1/2, 500/1000, +1, N°2, JUMBO, XXL)"),
    Infos_Brutes STRING OPTIONS(description="Concaténation de tous les attributs trouvés pour ne rien rater"),
    
    -- Prix
    Prix FLOAT64 OPTIONS(description="Prix unitaire"),
    
    -- Métadonnées
    Date DATE OPTIONS(description="Date du tarif (YYYY-MM-DD)"),
    Vendor STRING OPTIONS(description="Fournisseur: Hennequin"),
    
    -- Audit
    import_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP() OPTIONS(description="Date d'import")
)
PARTITION BY Date
CLUSTER BY Categorie, Methode_Peche
OPTIONS(
    description="Table structurée des tarifs Hennequin avec attributs enrichis",
    labels=[("vendor", "hennequin"), ("type", "structured")]
);

-- Note: BigQuery ne supporte pas les index explicites, mais le clustering aide
