

WITH filtered_data AS (
    SELECT
        MateriauxID,
        PrixUnitaire,
        Quantite, 
    FROM `projet-bi-isen.dataWarehouse.f_CommandeFournisseur` 
),
aggregated_data AS (
    SELECT
        MateriauxID,
        SUM(PrixUnitaire * Quantite) AS somme_ponderee_prix,
        SUM(Quantite) AS somme_quantite
    FROM filtered_data
    GROUP BY MateriauxID
),
ste AS(
    SELECT
    MateriauxID,
    CASE
        WHEN somme_quantite > 0 THEN ROUND(somme_ponderee_prix / somme_quantite,2)
        ELSE NULL
    END AS ValeurEstimee
    FROM aggregated_data
),
existing_data AS (
    SELECT 
        mat.MateriauxID,
        mat.MateriauxNom,
        s.ValeurEstimee
    FROM `projet-bi-isen`.`dataWarehouse`.`d_Materiaux` mat  -- Table actuelle d_Materiaux
    LEFT JOIN ste s ON mat.MateriauxID = s.MateriauxID
),
newDataValeruEstimee AS (
    SELECT
        materiaux_nom,
        ROUND(SUM(quantite_provisionnee*prix_provision)/SUM(quantite_provisionnee),2) AS ValeurEstimee
    FROM `projet-bi-isen`.`dataWarehouse`.`stg_CommandeFournisseur`
    GROUP BY materiaux_nom
),
new_data AS (
    SELECT
        GENERATE_UUID() AS MateriauxID,
        comf.materiaux_nom AS MateriauxNom,
        nd.ValeurEstimee AS ValeurEstimee
    FROM `projet-bi-isen`.`dataWarehouse`.`stg_CommandeFournisseur` comf
    LEFT JOIN newDataValeruEstimee nd ON comf.materiaux_nom = nd.materiaux_nom
    GROUP BY comf.materiaux_nom, nd.ValeurEstimee
),
unioned_data AS (
    SELECT
        MateriauxID,
        MateriauxNom,
        ValeurEstimee
    FROM existing_data
    UNION ALL
    SELECT
        MateriauxID,
        MateriauxNom,
        ValeurEstimee
    FROM new_data
    WHERE MateriauxNom NOT IN (SELECT MateriauxNom FROM existing_data)
)

SELECT 
    MateriauxID,
    MateriauxNom,
    ValeurEstimee
FROM unioned_data