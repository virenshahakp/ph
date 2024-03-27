WITH 

watched_minutes AS (

  SELECT * FROM {{ ref('fct_watched_minutes') }} 

)

, guide AS (

  SELECT * FROM {{ ref('dim_guide_metadata') }} 

)

, hours AS (

  SELECT
    user_id
    , show_title
    , channel_name
    , DATE_TRUNC('month', received_at)::DATE AS month  -- noqa: L029
    , SUM(minutes / 60.0) AS hours_per_show
  FROM  watched_minutes
  WHERE minutes > 0
    AND (played_asset_id  IN 
      (SELECT asset_id FROM guide WHERE has_public_view = TRUE) 
      OR requested_asset_id IN 
      (SELECT asset_id FROM guide WHERE has_public_view = TRUE))
  {{ dbt_utils.group_by(n=4) }}

)

SELECT
  user_id
  , month
  , show_title
  , channel_name
  , NULLIF(SUM(hours_per_show), 0) AS hours
FROM hours
{{ dbt_utils.group_by(n=4) }}