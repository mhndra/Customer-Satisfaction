{{
  config(
    unique_key='`Customer Satisfaction Key`'
  )
}}

WITH google_play_reviews AS (

  SELECT *
  FROM {{ source('bronze', 'google_play_reviews') }}
  WHERE appId = 'com.tiket.gits'

), enriched_gplay_reviews AS(
  
  SELECT * FROM {{ source('silver', 'enriched_gplay_reviews') }}

), enriched_reviews AS (

  SELECT
    g.reviewId AS `Review ID`,
    g.userName AS `User Name`,
    g.appId AS `App ID`,
    COALESCE(g.appVersion, 'Unidentified') AS `App Version`,
    e.normalized_root_cause AS `Root Cause`,
    CAST(g.at AS DATE) AS `Review Date (UTC)`,
    g.score AS `Satisfaction Score`
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
        '`Review ID`',
        '`User Name`',
        '`App ID`',
        '`App Version`',
        '`Root Cause`',
        '`Review Date (UTC)`'
      ])
    }} AS `Customer Satisfaction Key`,
    `Review ID`,
    `User Name`,
    `App ID`,
    `App Version`,
    `Root Cause`,
    `Review Date (UTC)`,
    `Satisfaction Score`
  FROM
    enriched_reviews

), joined_dimension_tables AS (

  SELECT
    k.*,
    r.`Review Key`,
    u.`User Key`,
    a.`App Key`,
    rc.`Root Cause Key`
  FROM
    keyed AS k
  LEFT JOIN
    {{ ref('dim_reviews') }} AS r
  ON
    k.`Review ID` = r.`Review ID`
  LEFT JOIN
    {{ ref('dim_users') }} AS u
  ON
    k.`User Name` = u.`User Name`
  LEFT JOIN
    {{ ref('dim_apps') }} AS a
  ON
    k.`App ID` = a.`App ID`
  AND
    k.`App Version` <=> a.`App Version`
  LEFT JOIN
    {{ ref('dim_root_causes') }} AS rc
  ON
    k.`Root Cause` = rc.`Root Cause`

), final AS (

  SELECT
    `Customer Satisfaction Key`,
    `Review Key`,
    `User Key`,
    `App Key`,
    `Root Cause Key`,
    `Review Date (UTC)`,
    `Satisfaction Score`
  FROM
    joined_dimension_tables

)

SELECT * FROM final