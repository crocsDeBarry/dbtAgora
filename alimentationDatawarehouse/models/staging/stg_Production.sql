-- models/staging/stg_Production.sql

WITH raw_data AS (
    SELECT
        ReleveProductionID AS production_id,
        Entrepot AS entrepot_nom,
        dateDebut AS production_debut,
        dateFin AS production_fin,
        produitProduit AS produit_nom,
        quantiteProduites AS quantite_produite,
        materiauxUtilise AS materiaux_utilises,
        quantiteUtilise AS quantite_utilisee
    FROM {{ source('ODS', 'f_listeProduction') }}
    WHERE ingestionTimestamp BETWEEN '{{ var("execution_date") }}' AND CURRENT_TIMESTAMP()
)

SELECT *
FROM raw_data
