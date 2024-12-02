-- back compat for old kwarg name
  
  
        
    

    

    merge into `projet-bi-isen`.`dataWarehouse`.`d_Entrepot` as DBT_INTERNAL_DEST
        using (WITH base AS (
    SELECT
        provisionnement_id,
        entrepot_nom AS EntrepotNom,
        entrepot_adresse AS EntrepotAdresse,
        entrepot_description AS EntrepotDescription,
        ROW_NUMBER() OVER (
            PARTITION BY entrepot_nom, entrepot_adresse 
            ORDER BY provisionnement_id
        ) AS rn -- Numérote chaque entrepôt pour éliminer les doublons
    FROM `projet-bi-isen`.`dataWarehouse`.`stg_CommandeFournisseur`
)

SELECT
    GENERATE_UUID() AS EntrepotID,
    EntrepotNom,
    EntrepotAdresse,
    EntrepotDescription
FROM base
WHERE rn = 1 -- Garde uniquement le premier entrepôt par groupe

    AND NOT EXISTS (
        SELECT 1
        FROM `projet-bi-isen`.`dataWarehouse`.`d_Entrepot` AS target
        WHERE target.EntrepotNom = base.EntrepotNom
          AND target.EntrepotAdresse = base.EntrepotAdresse
    )

        ) as DBT_INTERNAL_SOURCE
        on (FALSE)

    

    when not matched then insert
        (`EntrepotID`, `EntrepotNom`, `EntrepotAdresse`, `EntrepotDescription`)
    values
        (`EntrepotID`, `EntrepotNom`, `EntrepotAdresse`, `EntrepotDescription`)


    