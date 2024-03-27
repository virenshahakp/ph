{{ config(materialized='table', dist='user_id') }}

WITH

stream_starts AS (

  SELECT * FROM {{ ref('fct_stream_starts') }}

)

, watched_minutes AS (

  SELECT * FROM {{ ref('fct_watched_minutes') }}

)

, users AS (

  SELECT * FROM {{ ref('dim_users') }}

)

, stream_starts_per_user_per_platform AS (

  SELECT 
    user_id
    , platform
    , COUNT(1) AS streams
  FROM stream_starts
  {{ dbt_utils.group_by(n=2) }}

)

, user_streams_platform_rank AS (

  SELECT
    user_id
    , platform
    , streams
    , SUM(streams) OVER (PARTITION BY user_id) AS total_stream_starts
    , ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY streams DESC) AS platform_streams_rank
  FROM stream_starts_per_user_per_platform

)

, minutes_per_platform AS (

  SELECT
    user_id
    , platform
    , SUM(minutes) AS platform_minutes
    , COUNT(DISTINCT DATE_TRUNC('day', timestamp_start)) AS platform_days_used
  FROM watched_minutes
  {{ dbt_utils.group_by(n=2) }}

)

, user_minutes_platform_rank AS (

  SELECT
    user_id
    , platform
    , platform_minutes
    , platform_days_used
    , SUM(platform_minutes) OVER (PARTITION BY user_id) AS total_minutes
    , ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY platform_minutes DESC) AS platform_minutes_rank
  FROM minutes_per_platform

)

SELECT
  users.user_id
  , COALESCE(user_streams_platform_rank.platform, user_minutes_platform_rank.platform) AS platform
  , user_streams_platform_rank.streams AS stream_starts
  , user_streams_platform_rank.total_stream_starts
  , user_streams_platform_rank.platform_streams_rank
  , NULLIF(user_streams_platform_rank.streams, 0) / NULLIF(user_streams_platform_rank.total_stream_starts, 0)::float AS stream_percentage
  , user_minutes_platform_rank.platform_minutes
  , user_minutes_platform_rank.platform_days_used
  , user_minutes_platform_rank.total_minutes
  , user_minutes_platform_rank.platform_minutes_rank
  , NULLIF(user_minutes_platform_rank.platform_minutes, 0) / NULLIF(user_minutes_platform_rank.total_minutes, 0)::float AS minutes_percentage
FROM users
LEFT JOIN user_streams_platform_rank ON (users.user_id = user_streams_platform_rank.user_id)
LEFT JOIN user_minutes_platform_rank ON (
  users.user_id = user_minutes_platform_rank.user_id 
  AND (
    user_streams_platform_rank.platform = user_minutes_platform_rank.platform
    OR user_streams_platform_rank.platform IS NULL
  )
)