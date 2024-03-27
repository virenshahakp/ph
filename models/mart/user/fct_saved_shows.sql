{{ config(materialized='table', dist='user_id', sort=['show_id', 'save_at', 'unsave_at']) }}

WITH 

save_unsave_shows AS (

  SELECT * FROM {{ ref('saved_show_ranges_stage') }}
  
)

SELECT 
  user_id
  , show_id
  , save_at
  , unsave_at
FROM save_unsave_shows
