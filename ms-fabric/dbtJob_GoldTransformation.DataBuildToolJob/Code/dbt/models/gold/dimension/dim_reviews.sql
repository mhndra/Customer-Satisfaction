{{
  config(
    materialized='table'
  )
}}

WITH google_play_reviews AS (

  SELECT * 
  FROM {{ source('bronze', 'google_play_reviews') }}
  WHERE appId = 'com.tiket.gits'

), enriched_gplay_reviews AS(
  
  SELECT * FROM {{ source('silver', 'enriched_gplay_reviews') }}

), distinction AS (

  SELECT DISTINCT
    g.reviewId AS `Review ID`,
    COALESCE(g.reviewCreatedVersion, 'Unidentified') AS `Review Created Version`,
    g.content AS Review,
    e.has_complaint AS `Has Complaint`,
    e.primary_theme AS `Primary Theme`,
    e.secondary_themes AS `Secondary Themes`,
    e.reasoning AS Reasoning
  FROM
    google_play_reviews AS g
  LEFT JOIN
    enriched_gplay_reviews AS e
  ON
    g.reviewId = e.reviewId

), keyed AS (

  SELECT
    {{ 
      dbt_utils.generate_surrogate_key([
        '`Review ID`'
      ]) 
    }} AS `Review Key`,
    `Review ID`,
    `Review Created Version`,
    Review,
    `Has Complaint`,
    `Primary Theme`,
    `Secondary Themes`,
    Reasoning
  FROM
    distinction

), final AS (

  SELECT
    `Review Key`,
    `Review ID`,
    `Review Created Version`,
    Review,
    `Has Complaint`,
    `Primary Theme`,
    `Secondary Themes`,
    Reasoning
  FROM
    keyed

)

SELECT * FROM final