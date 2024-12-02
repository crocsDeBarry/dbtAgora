WITH base AS (
    SELECT
        DISTINCT livreur_nom AS LivreurNom,
    FROM `projet-bi-isen`.`dataWarehouse`.`stg_CommandeClient`
)

-- Si le modèle est exécuté en mode 'incremental', dbt ajoute seulement les données non présentes dans la table
SELECT
    GENERATE_UUID() AS LivreurID,
    LivreurNom
FROM base


    -- Filtrage des nouveaux clients qui ne sont pas déjà dans d_Client
    WHERE NOT EXISTS (
        SELECT 1
        FROM `projet-bi-isen`.`dataWarehouse`.`d_Livreur` AS target
        WHERE target.LivreurNom = base.LivreurNom
    )
