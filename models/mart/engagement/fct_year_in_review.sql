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

, watched as (

  select 
    fct_watched_minutes.*
    , meta.has_public_view
    , meta.content_type
    , meta.is_premium
    , meta.show_status
    , accounts.account_id
    , date_trunc('year', convert_timezone('America/New_York', 'UTC', fct_watched_minutes.timestamp_start)) as watch_year
  from fct_watched_minutes 
  join accounts on (fct_watched_minutes.user_id = accounts.user_id)
  left join meta on (fct_watched_minutes.requested_asset_id = meta.asset_id)
  where fct_watched_minutes.timestamp_start between 
    convert_timezone('America/New_York', 'UTC', '2018-01-01 00:00:00')
    and convert_timezone('America/New_York', 'UTC', '2022-12-31 23:59:59')
  order by fct_watched_minutes.timestamp_start asc

)

, aggs as (

  select
    watched.*
    , row_number() over (partition by account_id, watch_year) as sequence -- noqa:disable=L029
    , count(
      id
    ) over (
      partition by philo_series_id, account_id, watch_year order by timestamp_start rows unbounded preceding
    ) as series_watch_number
    , count(
      id
    ) over (
      partition by
        philo_series_id, account_id, watch_year
      order by timestamp_start rows between unbounded preceding and unbounded following
    ) as total_series_watches
    , min(timestamp_start) over (partition by philo_series_id, account_id, watch_year) as first_watched_at
    , max(timestamp_start) over (partition by philo_series_id, account_id, watch_year) as last_watched_at
    , sum(minutes) over (partition by channel_name, account_id, watch_year) as channel_minutes
    , sum(minutes) over (partition by philo_series_id, account_id, watch_year) as series_minutes
  from watched

)

select
  aggs.*
  -- , MAX(series_watch_number) OVER (PARTITION BY philo_series_id) AS total_series_watches
  , coalesce(series_title, show_title) as content_title
  , dense_rank() over (partition by account_id, watch_year order by total_series_watches desc) as series_watches_dense_rank -- noqa:disable=L016
  , dense_rank() over (partition by account_id, watch_year order by series_minutes desc) as series_dense_rank
  , row_number() over (partition by account_id, watch_year order by series_minutes desc) as series_rank
  , dense_rank() over (partition by account_id, watch_year order by channel_minutes desc) as channel_dense_rank  
  , row_number() over (partition by account_id, watch_year order by channel_minutes desc) as channel_rank
  , row_number() over (partition by account_id, watch_year order by first_watched_at asc) as first_watch_order_asc
  , row_number() over (partition by account_id, watch_year order by first_watched_at desc) as first_watch_order_desc
  , row_number() over (partition by account_id, watch_year order by last_watched_at asc) as last_watch_order_asc
  , row_number() over (partition by account_id, watch_year order by last_watched_at desc) as last_watch_order_desc
from aggs