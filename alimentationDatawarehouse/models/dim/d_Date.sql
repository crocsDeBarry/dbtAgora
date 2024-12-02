WITH date_Client AS (
    SELECT DISTINCT
        date_commande AS TriDate
    FROM {{ ref('stg_CommandeClient') }}
),
date_CommandeFournisseur AS (
    SELECT DISTINCT
        date_commande AS TriDate
    FROM {{ ref('stg_CommandeFournisseur') }}
    UNION ALL
    SELECT DISTINCT
        date_livraison AS TriDate
    FROM {{ ref('stg_CommandeFournisseur') }}
),
date_Production AS (
    SELECT DISTINCT
        production_debut AS TriDate
    FROM {{ ref('stg_Production') }}
    UNION ALL
    SELECT DISTINCT
        production_fin AS TriDate
    FROM {{ ref('stg_Production') }}
),
mixDate AS (
    SELECT DISTINCT
        TriDate
    FROM date_Client
    UNION ALL
    SELECT DISTINCT
        TriDate
    FROM date_CommandeFournisseur
    UNION ALL
    SELECT DISTINCT
        TriDate
    FROM date_Production
),
triDoublon AS (
    SELECT DISTINCT
        TriDate
    FROM mixDate
)

SELECT
    GENERATE_UUID() AS DateID,          -- Génération d'un identifiant unique pour chaque date
    EXTRACT(YEAR FROM TriDate) AS Annee,
    EXTRACT(MONTH FROM TriDate) AS Mois,
    EXTRACT(DAY FROM TriDate) AS Jours,
    TriDate AS DateViz
FROM triDoublon

{% if is_incremental() %}
-- Filtrage des nouvelles dates qui ne sont pas déjà présentes dans la table cible
WHERE NOT EXISTS (
    SELECT 1
    FROM {{ this }} AS target
    WHERE 
        target.Annee = EXTRACT(YEAR FROM TriDate) AND
        target.Mois = EXTRACT(MONTH FROM TriDate) AND
        target.Jours = EXTRACT(DAY FROM TriDate)
)
{% endif %}