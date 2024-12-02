

WITH raw_data AS (
    SELECT
        ProvisionnementID AS provisionnement_id,
        FournisseurNom AS fournisseur_nom,
        FournisseurContact AS fournisseur_contact,
        FournisseurAdresse AS fournisseur_adresse,
        EntrepotNom AS entrepot_nom,
        EntrepotAdresse AS entrepot_adresse,
        EntrepotDescription AS entrepot_description,
        DateCommande AS date_commande,
        DateLivraison AS date_livraison,
        Materiaux AS materiaux_nom,
        Quantite AS quantite_provisionnee,
        Prix AS prix_provision,
        QualiteProvision AS qualite_provision
    FROM {{ source('ODS', 'f_approvisionnement') }}
    WHERE ingestionTimestamp BETWEEN '{{ var("execution_date") }}' AND CURRENT_TIMESTAMP()
)

SELECT *
FROM raw_data
