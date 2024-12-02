-- a modifié

WITH base AS (
    SELECT
        transaction_id,
        client_nom AS Nom,
        client_prenom AS Prenom,
        client_adresse AS Adresse,
        client_contact AS Contact,
        ROW_NUMBER() OVER (
            PARTITION BY client_nom, client_prenom, client_adresse, client_contact 
            ORDER BY transaction_id
        ) AS rn
    FROM `projet-bi-isen`.`dataWarehouse`.`stg_CommandeClient`
)

-- Si le modèle est exécuté en mode 'incremental', dbt ajoute seulement les données non présentes dans la table
SELECT
    GENERATE_UUID() AS ClientID,
    Nom,
    Prenom,
    Adresse,
    Contact
FROM base
WHERE rn = 1 -- Garde uniquement le premier entrepôt par groupe

    -- Filtrage des nouveaux clients qui ne sont pas déjà dans d_Client
    AND NOT EXISTS (
        SELECT 1
        FROM `projet-bi-isen`.`dataWarehouse`.`d_Client` AS target
        WHERE target.Nom = base.Nom
          AND target.Prenom = base.Prenom
          AND target.Adresse = base.Adresse
          AND target.Contact = base.Contact
    )
