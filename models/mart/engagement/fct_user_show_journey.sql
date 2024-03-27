{{ 
  config(
    materialized='incremental'
    , dist='user_id'
    , sort=['user_id', 'seq']
    , unique_key='user_id'
  )
}}

{%- set max_processed_at = incremental_max_value('dbt_processed_at') %}

with

processed_users as (

  select distinct user_id
  from {{ ref('fct_watched_minutes') }}
  {% if is_incremental() %}
    where dbt_processed_at > {{ max_processed_at }}
  {% endif %}

)

, watched_minutes as (

  select *
  from {{ ref('fct_watched_minutes') }}
  where user_id in (select user_id from processed_users)

)

, show_saves as (

  -- instead of incremental logic, process only the subset of users we need to update
  select
    user_id
    , show_id
  from {{ ref('fct_saved_shows') }}
  where user_id in (select user_id from processed_users)

)

, user_show_saves as (

  select
    user_id
    , show_id
    , count(1) as saves
  from show_saves
  {{ dbt_utils.group_by(n=2) }}

)

, series_channel_summary as (

  select
    watched_minutes.user_id
    , watched_minutes.channel_callsign
    , watched_minutes.channel_name
    , watched_minutes.channel_id
    , watched_minutes.show_id
    , watched_minutes.philo_series_id
    , user_show_saves.saves                                              as saves
    , coalesce(watched_minutes.series_title, watched_minutes.show_title) as show_title
    , min(watched_minutes.timestamp_start)                               as first_watched_at
    , max(watched_minutes.timestamp_start)                               as last_watched_at
    , count(distinct watched_minutes.episode_id)                         as episodes_watched
    , sum(watched_minutes.minutes / 60.0)                                as hours_watched
    , max(watched_minutes.dbt_processed_at)                              as dbt_processed_at
  from watched_minutes
  left join user_show_saves
    on (
      watched_minutes.user_id = user_show_saves.user_id
      and watched_minutes.show_id = user_show_saves.show_id
    )
  {{ dbt_utils.group_by(n=8) }}

)


select
  user_id
  , channel_callsign
  , channel_name
  , channel_id
  , show_id
  , show_title
  , philo_series_id
  , first_watched_at
  , last_watched_at
  , episodes_watched
  , hours_watched
  , dbt_processed_at
  , isnull(saves, 0) as saves
  , row_number() over (
    partition by user_id
    order by first_watched_at
  )                  as seq
from series_channel_summary