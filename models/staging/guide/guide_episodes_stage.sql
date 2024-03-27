{{ config(materialized='ephemeral') }}
WITH 

episodes AS (

  SELECT * FROM {{ ref('guide_episodes_source') }}

)

-- There are duplicates in the source data with the same id, asset_id, and title
-- which we need to clean up. For now, de-duplicate here. There are also
-- records for the same id and asset_id but with and without and episode title.
, dedup_title AS (

  SELECT 
    episode_id
    , asset_id
    , original_air_date
    , episode_num
    , season_num
    , LAST_VALUE(episode_title) OVER (
      PARTITION BY 
        episode_id
        , asset_id 
      ORDER BY created_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS episode_title
  FROM episodes

)

SELECT DISTINCT * FROM dedup_title
