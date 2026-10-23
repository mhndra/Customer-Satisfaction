{{
  config(
    materialized='table'
  )
}}

WITH google_play_reviews AS (

  SELECT * 
  FROM {{ source('bronze', 'google_play_reviews') }}
  WHERE appId = 'com.tiket.gits'

), distinction AS (

  SELECT DISTINCT
    userName AS `User Name`,
    MAX_BY(userImage, at) AS `User Image`
  FROM
    google_play_reviews
  GROUP BY
    userName

), keyed AS (

  SELECT
    {{ 
      dbt_utils.generate_surrogate_key([
        '`User Name`'
      ]) 
    }} AS `User Key`,
    `User Name`,
    `User Image`
  FROM
    distinction

), final AS (

  SELECT
    `User Key`,
    `User Name`,
    `User Image`
  FROM
    keyed

)

SELECT * FROM final