{{ 
  config(
    materialized='incremental'
    , dist='user_id'
    , sort='watched_at_est'
    , unique_key='playback_session_id'
  )
}}

{%- set max_processed_at = incremental_max_value('dbt_processed_at') %}

/* This model is meant to summarize viewing at half-hour intervals as the lowest
   level of granularity that can be extracted from the reporting.

   Incrementality is achieved by mimicing the watched minutes processing and replacing
   the records at a playback session level if the record was updated by our watched
   minutes process.
*/

with

watched_minutes as (

  select *
  from {{ ref('fct_watched_minutes') }}
  {%- if is_incremental() %}
    where dbt_processed_at > {{ max_processed_at }}
  {%- endif %}

)

, public_channels as (

  select channel_callsign
  from {{ ref('dim_channels') }}
  where has_public_view is true

)

, users as (

  select
    dim_users.account_id
    , dim_users.user_id
    , dim_accounts.dma_code
    , dim_accounts.dma_name
    , dim_accounts.demographic_gender
    , dim_accounts.demographic_age_range
    , dim_accounts.demographic_income
    , coalesce(dim_accounts.first_payment_at is not null, false) as has_paid
  from {{ ref('dim_users') }}
  join {{ ref('dim_accounts') }} on (dim_users.account_id = dim_accounts.account_id)
  where dim_accounts.is_billable is true

)

, watched_hours as (

  select
    watched_minutes.playback_session_id
    , watched_minutes.channel_callsign
    , watched_minutes.channel_name
    , watched_minutes.show_title
    , watched_minutes.episode_title
    , watched_minutes.user_id
    , users.account_id
    , users.has_paid
    , users.dma_code
    , users.dma_name
    , users.demographic_gender
    , users.demographic_age_range
    , users.demographic_income
    , watched_minutes.platform
    , watched_minutes.asset_type
    , watched_minutes.is_paid_programming
    , watched_minutes.channel_id
    , watched_minutes.show_id
    , watched_minutes.episode_id
    , watched_minutes.philo_series_id
    , watched_minutes.dbt_processed_at
    /*
     the following truncates to hourly data and then the date part component adds a 30 minute interval
     when the timestamp_start is in the second half of the hour to effectively give us a date_trunc
     at the half-hour granularity

    */
    , dateadd(
      minute
      , (date_part('minute', watched_minutes.timestamp_start)::int / 30) * 30                     -- results in 0 or 30
      , date_trunc('hour', convert_timezone('America/New_York', watched_minutes.timestamp_start)) -- add to truncated hour
    )                                                as watched_at_est

    , count(1)                                       as playbacks
    , sum(
      case when watched_minutes.asset_type = 'CHANNEL' then watched_minutes.minutes else 0.0 end
    ) / 60.0                                         as total_live_hours
    , sum(
      case when watched_minutes.asset_type = 'RECORDING' then watched_minutes.minutes else 0.0 end
    ) / 60.0                                         as total_dvr_hours
    , sum(
      case when watched_minutes.asset_type = 'LOOKBACK' then watched_minutes.minutes else 0.0 end
    ) / 60.0                                         as total_look_back_hours
    , sum(
      case when watched_minutes.asset_type = 'VOD' then watched_minutes.minutes else 0.0 end
    ) / 60.0                                         as total_vod_hours
    , nullif(sum(watched_minutes.minutes) / 60.0, 0) as total_hours
  from watched_minutes
  join users on (watched_minutes.user_id = users.user_id)
  join public_channels on (watched_minutes.channel_callsign = public_channels.channel_callsign)
  where watched_minutes.minutes > 0
  {{ dbt_utils.group_by(n=22) }}

)

select * from watched_hours