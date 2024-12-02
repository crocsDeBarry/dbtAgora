
  
    

    create or replace table `projet-bi-isen`.`dataWarehouse`.`stg_CommandeFournisseur`
      
    
    

    OPTIONS()
    as (
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
    FROM `projet-bi-isen`.`ODS`.`f_approvisionnement`
    WHERE ingestionTimestamp BETWEEN '2024-12-01' AND CURRENT_TIMESTAMP()
)

SELECT *
FROM raw_data
    );
  