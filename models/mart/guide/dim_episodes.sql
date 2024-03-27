{{ config(materialized='view') }}

WITH

all_guide_data AS (

  SELECT * FROM {{ ref('dim_guide_metadata' ) }}

)

SELECT
  episode_id
  , episode_title
  , MIN(episode_num) AS episode_number
  , MIN(season_num) AS season_number
FROM all_guide_data
WHERE 
  episode_id IS NOT NULL
{{ dbt_utils.group_by(n=2) }}