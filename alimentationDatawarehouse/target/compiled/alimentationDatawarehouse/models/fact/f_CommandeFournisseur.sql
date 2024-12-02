WITH staged_data AS (
    SELECT
        provisionnement_id AS ProvisionnementID,
        fournisseur_nom,
        fournisseur_contact,
        fournisseur_adresse,
        entrepot_nom,
        entrepot_adresse,
        entrepot_description,
        date_commande AS DateCommandeProvision,
        date_livraison AS DateLivraisonProvision,
        materiaux_nom,
        quantite_provisionnee AS Quantite,
        prix_provision AS PrixUnitaire,
        qualite_provision AS QualiteProvision
    FROM `projet-bi-isen`.`dataWarehouse`.`stg_CommandeFournisseur`
),

mapped_data AS (
    SELECT
        sd.ProvisionnementID,
        dim_materiaux.MateriauxID,
        dim_fournisseur.FournisseurID,
        dim_entrepot.EntrepotID,
        dim_date.DateID AS DateCommandeProvision,
        dim_dateLivraison.DateID AS DateLivraisonProvision,
        sd.Quantite,
        sd.PrixUnitaire,
        sd.QualiteProvision
    FROM staged_data sd
    LEFT JOIN `projet-bi-isen`.`dataWarehouse`.`d_Materiaux` dim_materiaux
        ON dim_materiaux.MateriauxNom = sd.materiaux_nom
    LEFT JOIN `projet-bi-isen`.`dataWarehouse`.`d_FournisseurDetails` dim_fournisseur
        ON dim_fournisseur.FournisseurNom = sd.fournisseur_nom
    LEFT JOIN `projet-bi-isen`.`dataWarehouse`.`d_Entrepot` dim_entrepot
        ON dim_entrepot.EntrepotNom = sd.entrepot_nom
    LEFT JOIN `projet-bi-isen`.`dataWarehouse`.`d_Date` dim_date
        ON dim_date.Annee = EXTRACT(YEAR FROM sd.DateCommandeProvision)
        AND dim_date.Mois = EXTRACT(MONTH FROM sd.DateCommandeProvision)
        AND dim_date.Jours = EXTRACT(DAY FROM sd.DateCommandeProvision)
    LEFT JOIN `projet-bi-isen`.`dataWarehouse`.`d_Date` dim_dateLivraison
        ON dim_dateLivraison.Annee = EXTRACT(YEAR FROM sd.DateLivraisonProvision)
        AND dim_dateLivraison.Mois = EXTRACT(MONTH FROM sd.DateLivraisonProvision)
        AND dim_dateLivraison.Jours = EXTRACT(DAY FROM sd.DateLivraisonProvision)
)

SELECT
    ProvisionnementID,
    MateriauxID,
    FournisseurID,
    EntrepotID,
    DateCommandeProvision,
    DateLivraisonProvision,
    Quantite,
    PrixUnitaire,
    QualiteProvision
FROM mapped_data