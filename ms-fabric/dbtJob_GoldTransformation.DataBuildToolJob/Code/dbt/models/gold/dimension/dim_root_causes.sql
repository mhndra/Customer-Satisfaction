{{
  config(
    materialized='table'
  )
}}

WITH enriched_gplay_reviews AS(
  
  SELECT * FROM {{ source('silver', 'enriched_gplay_reviews') }}

), distinction AS (

  SELECT DISTINCT
    normalized_root_cause AS `Root Cause`,
    weight AS `Root Cause Weight`,
    cluster_id AS `Cluster ID`,
    cluster_name AS `Cluster Name`,
    cluster_description AS `Cluster Description`
  FROM
    enriched_gplay_reviews

), keyed AS (

  SELECT
    {{ 
      dbt_utils.generate_surrogate_key([
        '`Root Cause`'
      ]) 
    }} AS `Root Cause Key`,
    `Root Cause`,
    `Root Cause Weight`,
    `Cluster ID`,
    `Cluster Name`,
    `Cluster Description`
  FROM
    distinction

), final AS (

  SELECT
    `Root Cause Key`,
    `Root Cause`,
    `Root Cause Weight`,
    `Cluster ID`,
    `Cluster Name`,
    `Cluster Description`
  FROM
    keyed

)

SELECT * FROM final