{{ config(materialized='table', dist='user_id', sort='show_id') }}

WITH

guide AS (

  SELECT * FROM {{ ref('dim_guide_metadata') }}

)

, show_saves AS (

  SELECT * FROM {{ ref('fct_saved_shows') }}

)

, watched_minutes AS (

  SELECT * FROM {{ ref('fct_watched_minutes') }}

)

, user_show_saves AS (

  SELECT
    user_id
    , show_id
    , COUNT(1) AS saves
  FROM show_saves
  {{ dbt_utils.group_by(n=2) }}

)

, show_summary AS (

  SELECT
    watched_minutes.user_id
    , watched_minutes.show_id
    , user_show_saves.saves AS saves
    , MIN(watched_minutes.timestamp_start) AS first_watched_at
    , MAX(watched_minutes.timestamp) AS last_watched_at
    , COUNT(DISTINCT watched_minutes.episode_id) AS episodes_watched
    , SUM(watched_minutes.minutes/60.0) AS hours_watched
  FROM watched_minutes
  LEFT JOIN user_show_saves ON (
    watched_minutes.user_id = user_show_saves.user_id
    AND watched_minutes.show_id = user_show_saves.show_id
  )
  {{ dbt_utils.group_by(n=3) }}

)

SELECT * FROM show_summary
