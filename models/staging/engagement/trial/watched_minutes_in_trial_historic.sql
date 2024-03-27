{{
  config(
    materialized='incremental'
    , sort='created_at'
    , dist='account_id'
    , unique_key='account_id'
    , tags=["exclude_daily"]
  )
}}

{%- set max_created_at = incremental_max_value('created_at') %}

with

watched_ranges as (

  select * from {{ ref('fct_watched_minutes') }}

)

, accounts as (

  select
    dim_users.user_id
    , dim_users.account_id
    , dim_accounts.created_at
  from
    {{ ref('dim_accounts') }}
  join {{ ref('dim_users') }} on (
    dim_users.account_id = dim_accounts.account_id
  )

)


select
  accounts.account_id
  , accounts.created_at
  , count(distinct accounts.user_id)                   as profiles_used
  , sum(watched_ranges.minutes)                        as minutes
  , count(distinct watched_ranges.playback_session_id) as sessions
  , count(distinct watched_ranges.show_title)          as distinct_shows
  , count(distinct watched_ranges.platform)            as distinct_platforms
  , count(distinct date_trunc(
    'day'
    , convert_timezone(
      'America/New_York'
      , watched_ranges.timestamp_start
    )
  ))                                                   as active_days

from accounts
join watched_ranges
  on (
    accounts.user_id = watched_ranges.user_id
  )
where accounts.created_at < {{ dbt.dateadd('day', -incremental_recent_days(), 'current_date') }}
  {%- if is_incremental() %}
    and accounts.created_at > {{ max_created_at }}
  {%- endif %}
  and watched_ranges.timestamp_start between accounts.created_at and {{ dbt.dateadd('day', 7, 'accounts.created_at' ) }}
group by accounts.account_id, accounts.created_at
