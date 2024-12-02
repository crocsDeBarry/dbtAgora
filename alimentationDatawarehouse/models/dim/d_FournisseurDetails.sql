WITH base AS (
    SELECT
        DISTINCT provisionnement_id,
        fournisseur_nom AS FournisseurNom,
        fournisseur_contact AS FournisseurContact,
        fournisseur_adresse AS FournisseurAdresse,
        ROW_NUMBER() OVER (
            PARTITION BY fournisseur_nom, fournisseur_contact, fournisseur_adresse 
            ORDER BY provisionnement_id
        ) AS rn
    FROM {{ ref('stg_CommandeFournisseur') }}
)

-- Si le modèle est exécuté en mode 'incremental', dbt ajoute seulement les données non présentes dans la table
SELECT
    GENERATE_UUID() AS FournisseurID,
    FournisseurNom,
    FournisseurContact,
    FournisseurAdresse
FROM base
WHERE rn = 1
{% if is_incremental() %}
    -- Filtrage des nouveaux clients qui ne sont pas déjà dans d_Client
    AND NOT EXISTS (
        SELECT 1
        FROM {{ this }} AS target
        WHERE target.FournisseurNom = base.FournisseurNom
          AND target.FournisseurContact = base.FournisseurContact
          AND target.FournisseurContact = base.FournisseurContact
    )
{% endif %}
