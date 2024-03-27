{{ config(materialized="table", dist="account_id", sort="played_date", enabled=false) }}
WITH 

watch_mins AS (

  SELECT * FROM {{ ref('fct_watched_minutes') }}

)

, metadata AS (

  SELECT * FROM {{ ref('dim_guide_metadata') }}

)

, users AS (

  SELECT * FROM {{ ref('dim_users') }}

)

SELECT
  users.account_id
  , watch_mins.user_id
  , DATE_TRUNC('day', watch_mins.timestamp)::DATE AS played_date
  , SUM(watch_mins.minutes / 60.0) AS hours_per_user
FROM watch_mins
JOIN metadata ON (watch_mins.asset_id = metadata.asset_id)
JOIN users ON watch_mins.user_id = users.user_id
WHERE metadata.has_public_view IS TRUE
{{ dbt_utils.group_by(n=3) }}
