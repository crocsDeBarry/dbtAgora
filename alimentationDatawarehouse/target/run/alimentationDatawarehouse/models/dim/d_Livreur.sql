-- back compat for old kwarg name
  
  
        
    

    

    merge into `projet-bi-isen`.`dataWarehouse`.`d_Livreur` as DBT_INTERNAL_DEST
        using (WITH base AS (
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

        ) as DBT_INTERNAL_SOURCE
        on (FALSE)

    

    when not matched then insert
        (`LivreurID`, `LivreurNom`)
    values
        (`LivreurID`, `LivreurNom`)


    