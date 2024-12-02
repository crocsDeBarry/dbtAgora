WITH raw_data AS (
    SELECT
        transactionID AS transaction_id,
        nomClient AS client_nom,
        prenomClient AS client_prenom,
        adresseClient AS client_adresse,
        contactClient AS client_contact,
        dateCommande AS date_commande,
        livreur AS livreur_nom,
        statut AS statut_commande,
        produit AS produit_nom,
        quantite AS produit_quantite,
        prixUnite AS produit_prix_unitaire
    FROM `projet-bi-isen`.`ODS`.`f_commandeInternet` WHERE ingestionTimestamp BETWEEN '2024-12-01' AND CURRENT_TIMESTAMP()      --mettre en variable pour la date de début
)

SELECT *
FROM raw_data