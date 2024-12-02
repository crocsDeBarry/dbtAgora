WITH base AS (
    SELECT
        provisionnement_id,
        entrepot_nom AS EntrepotNom,
        entrepot_adresse AS EntrepotAdresse,
        entrepot_description AS EntrepotDescription,
        ROW_NUMBER() OVER (
            PARTITION BY entrepot_nom, entrepot_adresse 
            ORDER BY provisionnement_id
        ) AS rn -- Numérote chaque entrepôt pour éliminer les doublons
    FROM {{ ref('stg_CommandeFournisseur') }}
)

SELECT
    GENERATE_UUID() AS EntrepotID,
    EntrepotNom,
    EntrepotAdresse,
    EntrepotDescription
FROM base
WHERE rn = 1 -- Garde uniquement le premier entrepôt par groupe
{% if is_incremental() %}
    AND NOT EXISTS (
        SELECT 1
        FROM {{ this }} AS target
        WHERE target.EntrepotNom = base.EntrepotNom
          AND target.EntrepotAdresse = base.EntrepotAdresse
    )
{% endif %}
