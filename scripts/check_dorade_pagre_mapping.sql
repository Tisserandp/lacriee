-- Vérifier les mappings actuels pour DORADE et PAGRE
SELECT categorie_raw, decoupe, famille_std, espece_std
FROM `lacriee.PROD.Mapping_Categories`
WHERE UPPER(categorie_raw) LIKE '%DORADE%' OR UPPER(categorie_raw) LIKE '%PAGRE%'
ORDER BY categorie_raw, decoupe NULLS FIRST;
