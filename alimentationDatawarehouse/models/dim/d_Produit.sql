{{ config(materialized='table') }}

WITH filtered_data AS (
    SELECT
        prod.ProduitID,
        prod.QuantiteProduite,
        prod.MateriauxID,
        mat.ValeurEstimee AS PrixUnitaire,
        prod.QuantiteUtilise 
    FROM `projet-bi-isen.dataWarehouse.f_Production` prod
    LEFT JOIN {{ ref('d_Materiaux') }} mat ON prod.MateriauxID = mat.MateriauxID
),
aggregated_data AS (
    SELECT
        ProduitID,
        SUM(QuantiteProduite) AS somme_quantite_produite,
        SUM(PrixUnitaire * QuantiteUtilise) AS somme_ponderee_prix,
        SUM(QuantiteUtilise) AS somme_quantite
    FROM filtered_data
    GROUP BY ProduitID
),
ste AS (
    SELECT
        ProduitID,
        CASE
            WHEN somme_quantite > 0 THEN ROUND((somme_ponderee_prix / somme_quantite) / somme_quantite_produite, 2)
            ELSE NULL
        END AS CoutdeRevient
    FROM aggregated_data
),
existing_data AS (
    SELECT 
        prod.ProduitID,
        prod.ProduitNom,
        s.CoutdeRevient
    FROM {{ this }} prod  -- Table actuelle d_Produit
    LEFT JOIN ste s ON prod.ProduitID = s.ProduitID
),
newdataCoutDeRevient AS (
    SELECT
        prod.produit_nom,
        ROUND(SUM(mat.ValeurEstimee*prod.quantite_utilisee)/prod.quantite_produite,2) AS CoutdeRevient
    FROM {{ ref('stg_Production') }} prod
    LEFT JOIN {{ ref('d_Materiaux') }} mat ON prod.materiaux_utilises = mat.MateriauxNom
    GROUP BY produit_nom, prod.quantite_produite
),
new_data AS (
    SELECT
        GENERATE_UUID() AS ProduitID,
        prod.produit_nom AS ProduitNom,
        ncdt.CoutdeRevient AS CoutdeRevient
    FROM {{ ref('stg_Production') }} prod
    LEFT JOIN newdataCoutDeRevient ncdt ON prod.produit_nom = ncdt.produit_nom
    GROUP BY prod.produit_nom, ncdt.CoutdeRevient
),
unioned_data AS (
    SELECT
        ProduitID,
        ProduitNom,
        CoutdeRevient
    FROM existing_data
    UNION ALL
    SELECT
        ProduitID,
        ProduitNom,
        CoutdeRevient
    FROM new_data
    -- Amélioration de la condition pour éviter les NULL avec NOT IN
    WHERE ProduitNom NOT IN (SELECT ProduitNom FROM existing_data WHERE ProduitNom IS NOT NULL)
)

SELECT 
    ProduitID,
    ProduitNom,
    CoutdeRevient
FROM unioned_data
