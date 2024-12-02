WITH  __dbt__cte__d_StatutCommande as (


SELECT *
FROM `projet-bi-isen.dataWarehouse.d_StatutCommande`
), staged_data AS (
    SELECT
        client_nom,
        client_prenom,
        client_adresse,
        client_contact,
        produit_nom,
        livreur_nom,
        statut_commande,
        produit_prix_unitaire AS PrixProduitVente,
        produit_quantite AS Quantite,
        date_commande AS DateCommande
    FROM `projet-bi-isen`.`dataWarehouse`.`stg_CommandeClient`
),

mapped_data AS (
    SELECT
        GENERATE_UUID() AS CommandeID,
        -- Recherche des clés dans les tables de dimension
        dim_client.ClientID,
        dim_product.ProduitID,
        dim_livreur.LivreurID,
        dim_statut.StatutID,
        sd.PrixProduitVente,
        sd.Quantite,
        dim_date.DateID AS DateCommande
    FROM staged_data sd
    LEFT JOIN `projet-bi-isen`.`dataWarehouse`.`d_Client` dim_client
        ON dim_client.Nom = sd.client_nom
        AND dim_client.Prenom = sd.client_prenom
        AND dim_client.Adresse = sd.client_adresse
        AND dim_client.Contact = sd.client_contact
    LEFT JOIN `projet-bi-isen`.`dataWarehouse`.`d_Produit` dim_product
        ON dim_product.ProduitNom = sd.produit_nom
    LEFT JOIN `projet-bi-isen`.`dataWarehouse`.`d_Livreur` dim_livreur
        ON dim_livreur.LivreurNom = sd.livreur_nom
    LEFT JOIN __dbt__cte__d_StatutCommande dim_statut
        ON dim_statut.Statut = sd.statut_commande
    LEFT JOIN `projet-bi-isen`.`dataWarehouse`.`d_Date` dim_date
        ON dim_date.Annee = EXTRACT(YEAR FROM sd.DateCommande)
        AND dim_date.Mois = EXTRACT(MONTH FROM sd.DateCommande)
        AND dim_date.Jours = EXTRACT(DAY FROM sd.DateCommande)
)

SELECT
    CommandeID,
    ClientID,
    ProduitID,
    LivreurID,
    StatutID,
    PrixProduitVente,
    Quantite,
    DateCommande
FROM mapped_data