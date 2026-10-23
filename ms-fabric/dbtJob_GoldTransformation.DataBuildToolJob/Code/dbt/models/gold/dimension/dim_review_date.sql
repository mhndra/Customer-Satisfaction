{{
  config(
    materialized='table'
  )
}}

WITH google_play_reviews (

  SELECT *
  FROM {{ source('bronze', 'google_play_reviews') }}
  WHERE appId = 'com.tiket.gits'

), distinction AS (

  SELECT DISTINCT
    CAST(at AS DATE) AS `Review Date`
  FROM
    google_play_reviews

), enriched_date AS (

  SELECT
    `Review Date`,
    DATE_TRUNC('week', `Review Date`) AS `First Day of Week`,
    DATE_TRUNC('month', `Review Date`) AS `First Day of Month`,
    DAY(`Review Date`) AS Day,
    DATE_FORMAT(`Review Date`, 'MMM') AS Month,
    MONTH(`Review Date`) AS `Month Sorting`,
    'Qtr' || CAST(QUARTER(`Review Date`) AS STRING) AS Quarter,
    QUARTER(`Review Date`) AS `Quarter Sorting`,
    YEAR(`Review Date`) AS Year,
    DATE_FORMAT(`Review Date`, 'EEEE') AS `Day of Week`,
    WEEKDAY(`Review Date`) AS `Day of Week Sorting`
  FROM
    distinction

), final AS (

  SELECT
    `Review Date`,
    `First Day of Week`,
    `First Day of Month`,
    Day,
    Month,
    `Month Sorting`,
    Quarter,
    `Quarter Sorting`,
    Year,
    `Day of Week`,
    `Day of Week Sorting`
  FROM
    enriched_date

)

SELECT * FROM final