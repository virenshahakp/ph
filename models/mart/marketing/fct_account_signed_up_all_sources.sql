{{ config(materialized='view') }}
WITH

user_signed_up AS (

  SELECT * FROM {{ ref('fct_user_signed_up_all_sources') }}

)

, primary_users AS (

  SELECT 
    user_id 
  FROM {{ ref('rails_prod_users_stage') }}
  WHERE user_id = account_id

)

SELECT 
  user_signed_up.user_id
  , user_signed_up.user_id AS account_id
  , user_signed_up.anonymous_id
  , user_signed_up.platform
  , user_signed_up.signed_up_at
FROM user_signed_up
JOIN primary_users ON (user_signed_up.user_id = primary_users.user_id)
