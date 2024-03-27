{{ config(materialized='table', dist='user_id', sort='signed_up_at') }}
WITH

first_platform AS (

  SELECT * FROM {{ ref('all_users_first_platform') }}

)

SELECT 
  user_id
  , anonymous_id
  , platform
  , "timestamp" AS signed_up_at
FROM first_platform
