{{ config(materialized='view') }}
WITH 

user_platforms AS (

  SELECT * FROM {{ ref('fct_user_platforms') }}

)

, users AS (

  SELECT * FROM {{ ref('dim_users') }}

)

, accounts AS (

  SELECT * FROM {{ ref('dim_accounts') }}

)

, aggregate_user_to_account AS (

  SELECT 
    users.account_id
    , user_platforms.platform
    , SUM(stream_starts) AS stream_starts
    , SUM(platform_minutes) AS platform_minutes
  FROM users
  JOIN user_platforms ON (users.user_id = user_platforms.user_id)
  WHERE user_platforms.platform IS NOT NULL
  {{ dbt_utils.group_by(n=2) }}

)

, rank_platforms AS (

  SELECT
    account_id
    , platform
    , stream_starts
    , platform_minutes    
    , SUM(stream_starts) OVER (PARTITION BY account_id) AS total_stream_starts
    , SUM(platform_minutes) OVER (PARTITION BY account_id) AS total_minutes
    , ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY stream_starts DESC) AS platform_streams_rank
    , ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY platform_minutes DESC) AS platform_minutes_rank
  FROM aggregate_user_to_account

)

SELECT
  accounts.account_id
  , rank_platforms.platform
  , rank_platforms.stream_starts
  , rank_platforms.platform_minutes
  , rank_platforms.total_stream_starts
  , rank_platforms.total_minutes
  , rank_platforms.platform_streams_rank
  , rank_platforms.platform_minutes_rank
  , NULLIF(rank_platforms.stream_starts, 0) / NULLIF(rank_platforms.total_stream_starts, 0)::float AS stream_percentage
  , NULLIF(rank_platforms.platform_minutes, 0) / NULLIF(rank_platforms.total_minutes, 0)::float AS minutes_percentage
FROM accounts
LEFT JOIN rank_platforms ON (accounts.account_id = rank_platforms.account_id)