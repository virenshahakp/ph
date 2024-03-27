{{
  config(
    materialized='table'
    , dist='account_id'
    , tags=["exclude_daily", "exclude_hourly"]
  )
}}

with 

accounts as (

  select * from {{ ref('dim_users') }}

)

, fct_watched_minutes as (

  select * from {{ ref('fct_watched_minutes') }}

)

, meta as (

  select * from {{ ref('dim_guide_metadata') }}

)

, genres as (

  select * from {{ ref('fct_guide_genre_shows') }}

)

, watched as (

  select 
    fct_watched_minutes.*
    , meta.has_public_view
    , meta.content_type
    , meta.is_premium
    , meta.show_status
    , accounts.account_id
    , coalesce(genres.genre_name, 'Unlabelled') as genre_name
    , date_trunc('year', convert_timezone('America/New_York', 'UTC', fct_watched_minutes.timestamp_start)) as watch_year
  from fct_watched_minutes
  join accounts on (fct_watched_minutes.user_id = accounts.user_id)
  left join meta on (fct_watched_minutes.requested_asset_id = meta.asset_id)
  left join genres on (fct_watched_minutes.show_id = genres.show_id)
  where fct_watched_minutes.timestamp_start between 
    convert_timezone('America/New_York', 'UTC', '2018-01-01 00:00:00')
    and convert_timezone('America/New_York', 'UTC', '2022-12-31 23:59:59')
  order by fct_watched_minutes.timestamp_start asc

)

, agg as (

  select 
    genre_name
    , account_id
    , watch_year
    , sum(minutes) as total_genre_minutes
    , count(distinct philo_series_id) as shows
  from watched
  {{ dbt_utils.group_by(n=3) }}

)

select 
  account_id
  , genre_name
  , total_genre_minutes
  , shows
  , watch_year
  , sum(total_genre_minutes) over (partition by account_id, watch_year) as total_minutes
  , total_genre_minutes / nullif(sum(total_genre_minutes) over (partition by account_id, watch_year), 0) as genre_pct
  , row_number() over (partition by account_id, watch_year order by total_genre_minutes desc) as rank_minutes
  , row_number() over (partition by account_id, watch_year order by shows desc) as rank_shows
from agg