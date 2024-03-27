{{
  config(
    materialized='incremental'
    , dist='playback_session_id'
    , unique_key=['playback_session_id']
    , sort=['timestamp_start', 'playback_session_id']
    , on_schema_change='append_new_columns' 
  )
}}

{%- set max_ranges_dbt_processed_at = incremental_max_value('dbt_ranges_processed_at') %}
{%- set max_rebuff_dbt_processed_at = incremental_max_value('dbt_rebuff_processed_at') %}

{% set run_automatic = true %}

{% if ( var("start_date") != "" and var("end_date") != "" ) %}
  {% set run_automatic = false %}
{% endif %}

{% set dates = get_update_dates(2) %}
{% set start_date = dates.start_date %}
{% set end_date = dates.end_date %}


with

interval_durations_ids as (

  select
    playback_session_id
    , (position_stop - position_start) as interval_duration
  from {{ ref('fct_watched_ranges') }}
  where interval_duration > 0
    {% if is_incremental() and run_automatic %}
      and dbt_processed_at > {{ max_ranges_dbt_processed_at }}
    {% else %}
      and timestamp_start between '{{ start_date }}' and '{{ end_date }}'
    {% endif %}

)

, rebuff_ends_ids as (

  select playback_session_id
  from {{ ref('all_platforms_rebuffering_ends') }}
  where 1 = 1
    {% if is_incremental() and run_automatic %}
      and dbt_processed_at > {{ max_rebuff_dbt_processed_at }}
    {% else %}
      and event_timestamp between '{{ start_date }}' and '{{ end_date }}'
    {% endif %}

)

, modified_playback_sessions as (

  select playback_session_id
  from interval_durations_ids

  union all

  select playback_session_id
  from rebuff_ends_ids

)


, interval_durations as (

  select
    *
    , dbt_processed_at                 as dbt_ranges_processed_at
    , (position_stop - position_start) as interval_duration
  from {{ ref('fct_watched_ranges') }}
  where playback_session_id in (select playback_session_id from modified_playback_sessions)
  order by user_id, playback_session_id

)


, summed_watch_time as (

  select
    user_id
    , playback_session_id
    , platform
    , asset_type
    , asset_id
    , max(dbt_ranges_processed_at)            as dbt_ranges_processed_at
    , sum(interval_duration)                  as sum_watched_duration_seconds
    , sum(watched_seconds)                    as watched_seconds
    , min(date_trunc('day', timestamp_start)) as timestamp_start
  from interval_durations
  {{ dbt_utils.group_by(n=5) }}

)


, rebuff_ends as (

  select
    user_id
    , playback_session_id
    , platform
    , asset_id
    , max(dbt_processed_at) as dbt_rebuff_processed_at
    , count(1)              as rebuffering_events
    , sum(duration)         as rebuffering_duration
  from {{ ref('all_platforms_rebuffering_ends') }}
  where playback_session_id in (select playback_session_id from modified_playback_sessions)
  {{ dbt_utils.group_by(n=4) }}

)

select
  summed_watch_time.timestamp_start
  , summed_watch_time.user_id
  , summed_watch_time.playback_session_id
  , summed_watch_time.platform
  , summed_watch_time.asset_id
  , summed_watch_time.asset_type
  , summed_watch_time.sum_watched_duration_seconds
  , summed_watch_time.watched_seconds
  , summed_watch_time.dbt_ranges_processed_at
  , coalesce(rebuff_ends.dbt_rebuff_processed_at, '2000-01-01') as dbt_rebuff_processed_at
  , coalesce(rebuff_ends.rebuffering_events, 0)                 as rebuffering_events
  , coalesce(rebuff_ends.rebuffering_duration, 0)               as rebuffering_duration
from summed_watch_time
left join rebuff_ends
  on summed_watch_time.playback_session_id = rebuff_ends.playback_session_id
    and summed_watch_time.asset_id = rebuff_ends.asset_id
    and summed_watch_time.user_id = rebuff_ends.user_id