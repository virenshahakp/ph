{{ config(materialized='table', sort='show_episode_id', dist='ALL') }}

WITH

all_guide_data AS (

  SELECT * FROM {{ ref('dim_guide_metadata' ) }}

)

SELECT
  tms_series_id
  , show_title
  , show_id
  , episode_title
  , episode_id
  , original_air_date
  , content_type
  , root_show_id
  , show_run_time
  , philo_series_id
  , show_episode_id
  , BOOL_OR(has_public_view) AS has_public_view
FROM all_guide_data
WHERE 
  show_id IS NOT NULL
{{ dbt_utils.group_by(n=11) }}
