-- Vérifier que le mapping DORADE / PAGRE a été corrigé
SELECT categorie, espece_std, COUNT(*) as nb_produits
FROM `lacriee.PROD.Analytics_Produits_Comparaison`
WHERE vendor = 'Laurent Daniel'
  AND (categorie = 'DORADE / PAGRE' OR product_name LIKE '%Pagre%')
GROUP BY categorie, espece_std
ORDER BY categorie, espece_std;
