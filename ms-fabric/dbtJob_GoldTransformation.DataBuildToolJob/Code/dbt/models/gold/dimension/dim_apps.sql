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
    appId AS `App ID`,
    COALESCE(appVersion, 'Unidentified') AS `App Version`
  FROM
    google_play_reviews

), keyed AS (

  SELECT
    {{ 
      dbt_utils.generate_surrogate_key([
        '`App ID`',
        '`App Version`'
      ]) 
    }} AS `App Key`,
    `App ID`,
    `App Version`
  FROM
    distinction

), final AS (

  SELECT
    `App Key`,
    `App ID`,
    `App Version`
  FROM
    keyed

)

SELECT * FROM final