WITH staged_data AS (
    SELECT
        production_id AS ProductionID,
        entrepot_nom,
        production_debut AS DateDebut,
        production_fin AS DateFin,
        produit_nom,
        quantite_produite AS QuantiteProduite,
        materiaux_utilises,
        quantite_utilisee AS QuantiteUtilise
    FROM {{ ref('stg_Production') }}
),

mapped_data AS (
    SELECT
        sd.ProductionID,
        dim_produit.ProduitID,
        sd.QuantiteProduite,
        dim_materiaux.MateriauxID,
        sd.QuantiteUtilise,
        dim_entrepot.EntrepotID,
        dim_date_debut.DateID AS DateDebutID,
        dim_date_fin.DateID AS DateFinID
    FROM staged_data sd
    LEFT JOIN {{ ref('d_Produit') }} dim_produit
        ON dim_produit.ProduitNom = sd.produit_nom
    LEFT JOIN {{ ref('d_Materiaux') }} dim_materiaux
        ON dim_materiaux.MateriauxNom = sd.materiaux_utilises
    LEFT JOIN {{ ref('d_Entrepot') }} dim_entrepot
        ON dim_entrepot.EntrepotNom = sd.entrepot_nom
    LEFT JOIN {{ ref('d_Date') }} dim_date_debut
        ON dim_date_debut.Annee = EXTRACT(YEAR FROM sd.DateDebut)
        AND dim_date_debut.Mois = EXTRACT(MONTH FROM sd.DateDebut)
        AND dim_date_debut.Jours = EXTRACT(DAY FROM sd.DateDebut)
    LEFT JOIN {{ ref('d_Date') }} dim_date_fin
        ON dim_date_fin.Annee = EXTRACT(YEAR FROM sd.DateFin)
        AND dim_date_fin.Mois = EXTRACT(MONTH FROM sd.DateFin)
        AND dim_date_fin.Jours = EXTRACT(DAY FROM sd.DateFin)
)

SELECT
    ProductionID,
    ProduitID,
    QuantiteProduite,
    MateriauxID,
    QuantiteUtilise,
    EntrepotID,
    DateDebutID AS DateDebut,
    DateFinID AS DateFin
FROM mapped_data