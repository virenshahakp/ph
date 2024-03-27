{{ 
  config(
    materialized='view'
    , tags=["daily"]
  ) 
}}
with

standard_and_derived as (

  {{ dbt_utils.union_relations(
    relations=[
      ref('all_platforms_watched_ranges')
      , ref('all_platforms_derived_watched_ranges')
    ],
    include=[
      "user_id"
      , "playback_session_id"
      , "requested_asset_id"
      , "played_asset_id"
      , "asset_id"
      , "received_at"
      , "timestamp_start"
      , "timestamp"
      , "delay"
      , "position_start"
      , "position_stop"
      , "watched_seconds"
      , "hashed_session_id"
      , "context_ip"
      , "platform"
      , "channel_id"
      , "show_id"
      , "episode_id"
      , "run_time"
      , "tms_series_id"
      , "philo_series_id"
      , "asset_type"
      , "bitrate"
      , "watched_minutes_id"
      , "dbt_processed_at"
    ]
  ) }}

)

, guide_channels as (

  select * from {{ ref('dim_channels') }}

)

, guide_shows as (

  select * from {{ ref('dim_shows') }}

)

, guide_episodes as (

  select * from {{ ref('dim_episodes') }}

)

, guide_series as (

  select * from {{ ref('dim_guide_series') }}

)

select
  standard_and_derived.*
  , guide_channels.channel_name
  , guide_channels.channel_callsign
  , guide_shows.show_title
  , guide_episodes.episode_title
  , guide_series.series_title
from standard_and_derived
left join guide_channels
  on (
    standard_and_derived.channel_id = guide_channels.channel_id
  )
left join guide_shows
  on (
    standard_and_derived.show_id = guide_shows.show_id
  )
left join guide_episodes
  on (
    standard_and_derived.episode_id = guide_episodes.episode_id
  )
left join guide_series on (
  standard_and_derived.tms_series_id = guide_series.tms_series_id
  and standard_and_derived.philo_series_id = guide_series.philo_series_id
)
