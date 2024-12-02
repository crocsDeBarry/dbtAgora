{{ config(materialized='ephemeral') }}

SELECT *
FROM `projet-bi-isen.dataWarehouse.d_StatutCommande`