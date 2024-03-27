{{
  config(
    materialized='incremental'
    , unique_key='playback_session_id'
    , dist='user_id'
    , sort=['user_id', 'playback_session_id', 'dbt_processed_at', 'timestamp_start', 'watched_minutes_id']
  )
}}

{%- set max_processed_at = incremental_max_value('dbt_processed_at') %}

with

watched_ranges_to_process as (

  select playback_session_id
  from {{ ref('fct_watched_ranges') }}
  {%- if is_incremental() %}
    where dbt_processed_at > {{ max_processed_at }}
  {%- endif %}

)

, watched_ranges as (

  select *
  from {{ ref('fct_watched_ranges') }}
  where playback_session_id in (
      select playback_session_id from watched_ranges_to_process
    )

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

, saved_shows as (

  select * from {{ ref('fct_saved_shows') }}

)

, playback_sessions as (

  /*
  2022-09-21 SZ: we have identified that we are getting multiple records per
  playback session id in some scenarios (potentially philo connect targeting)
  and so are using the minimum start & maximum end if there are multiple events
  reported for a playback session
  */
  select
    playback_session_id
    , min(started_at) as started_at
    , max(
      case
        when ended_at is null or datediff('hours', started_at, ended_at) > 24
          then started_at + interval '24 hours'
        else ended_at
      end
    )                 as ended_at
  from {{ ref('fct_playback_sessions') }}
  where playback_session_id in (
      select playback_session_id from watched_ranges_to_process
    )
  {{ dbt_utils.group_by(n=1) }}

)

, wr_incremental as (

  select
    watched_minutes_id
    , user_id
    , playback_session_id
    , asset_id --legacy column name support
    , requested_asset_id
    , played_asset_id
    , platform
    , channel_id
    , show_id
    , episode_id
    , run_time
    , tms_series_id
    , philo_series_id
    , asset_type
    , timestamp_start
    , "timestamp"
    , received_at -- used to potentially override bad timestamps
    , dbt_processed_at
    , case
      when delay is null
        then 0
      else 1
    end                      as delay_count
    , watched_seconds / 60.0 as minutes
  from watched_ranges
  -- require that the data has joined to the guide data in order to be processed
  where asset_type is not null

)

, wm as (

  select
    watched_minutes_id
    , user_id
    , playback_session_id
    , asset_id --legacy column support
    , requested_asset_id
    , played_asset_id
    , platform
    -- count non-null delays; on ios/tvos platforms, delay shifts from null to non-null in one session
    , sum(delay_count) > 0  as is_live
    , sum(minutes)          as minutes
    , min(timestamp_start)  as timestamp_start -- min may not be needed any longer SZ: 2023-02
    , max("timestamp")      as "timestamp" -- max may not be needed any longer SZ: 2023-02
    , max(dbt_processed_at) as dbt_processed_at -- max may not be needed any longer SZ: 2023-02
    , max(received_at)      as received_at -- max may not be needed any longer SZ: 2023-02
  from wr_incremental
  {{ dbt_utils.group_by(n=7) }}

)

, wm_attributes as (

  -- use the most recent data for the asset attributes in the insert.
  -- these will always come from the incremental events, updating the 
  -- historic data if it exists with the latest guide data
  select
    wr_incremental.watched_minutes_id
    , wr_incremental.channel_id
    , wr_incremental.show_id
    , wr_incremental.episode_id
    , wr_incremental.run_time
    , wr_incremental.tms_series_id
    , wr_incremental.philo_series_id
    , wr_incremental.asset_type
    , guide_channels.channel_name
    , guide_channels.channel_callsign
    , guide_shows.show_title
    , guide_episodes.episode_title
    , guide_series.series_title
    -- if there is no joined data, default to false
    , coalesce(guide_shows.is_paid_programming, false) as is_paid_programming
  from wr_incremental
  left join guide_channels
    on (
      wr_incremental.channel_id = guide_channels.channel_id
    )
  left join guide_shows
    on (
      wr_incremental.show_id = guide_shows.show_id
    )
  left join guide_episodes
    on (
      wr_incremental.episode_id = guide_episodes.episode_id
    )
  left join guide_series on (
    wr_incremental.tms_series_id = guide_series.tms_series_id
    and wr_incremental.philo_series_id = guide_series.philo_series_id
  )

)

-- It may be possible for records in saved_shows to overlap and a simple left
-- join will double count watched ranges. Defend against this by counting the
-- number of overlapping saved_shows records.
, wm_with_saved_shows as (

  select
    wm.watched_minutes_id
    , wm.user_id
    , wm.playback_session_id
    , wm.asset_id -- legacy column support
    , wm.requested_asset_id
    , wm.played_asset_id
    , wm.platform
    , wm.is_live
    , wm.minutes
    , wm.timestamp_start
    , wm."timestamp"
    , wm.received_at
    , wm.dbt_processed_at
    , wm_attributes.channel_id
    , wm_attributes.show_id
    , wm_attributes.episode_id
    , wm_attributes.run_time
    , wm_attributes.tms_series_id
    , wm_attributes.philo_series_id
    , wm_attributes.asset_type
    , wm_attributes.channel_name
    , wm_attributes.channel_callsign
    , wm_attributes.show_title
    , wm_attributes.is_paid_programming
    , wm_attributes.episode_title
    , wm_attributes.series_title
    , count(saved_shows.user_id) as saved_shows_count
  from wm
  left join wm_attributes on (wm.watched_minutes_id = wm_attributes.watched_minutes_id)
  left join saved_shows
    on (
      saved_shows.user_id = wm.user_id
      and saved_shows.show_id = wm_attributes.show_id
      and wm.timestamp_start between saved_shows.save_at and coalesce(saved_shows.unsave_at, getdate())
    )
  {{ dbt_utils.group_by(n=26) }}

)

, watched_records as (

  select
    watched_minutes_id
    , user_id
    , playback_session_id
    , platform
    , asset_id --legacy column support
    , requested_asset_id
    , played_asset_id
    , channel_id
    , show_id
    , episode_id
    , run_time
    , tms_series_id
    , philo_series_id
    , channel_name
    , channel_callsign
    , show_title
    , is_paid_programming
    , episode_title
    , series_title
    , is_live
    , minutes
    , timestamp_start
    , "timestamp"
    , received_at
    , dbt_processed_at
    , case
      -- a recording is a lookback when a user has not saved the show
      when asset_type = 'RECORDING' and saved_shows_count = 0
        then 'LOOKBACK'
      else asset_type
    end as asset_type
  from wm_with_saved_shows

)

, adjusted_timestamps as (

  select
    watched_records.*
    , playback_sessions.started_at as session_started_at
    , playback_sessions.ended_at
    /*
    create an adjusted timestamp as client clocks can be wrong
    this requires the timestamp to respect our server playback session creation timestamps
    as a reporting constraint
    */
    , case
      -- 1. our timestamp_start from the client is consistent with the server clock
      when watched_records.timestamp_start between playback_sessions.started_at and playback_sessions.ended_at
        then watched_records.timestamp_start
      /*
      2. our received_at from segment is consistent with the server clock
      then use the later of playback session start or the time segment
      received the watched minutes data minus the duration of watching
      this ignores the duration a user may have paused content
      */
      when watched_records.received_at between playback_sessions.started_at and playback_sessions.ended_at
        then greatest(playback_sessions.started_at,{{ dbt.dateadd('minute', '-1*minutes::INT', 'received_at') }}) -- noqa: L019
      -- 3. else use playback session if it is known, or rely on the suspect client clock
      else coalesce(playback_sessions.started_at, watched_records.timestamp_start)
    end                            as timestamp_start_adjusted

    /*
    this uses the same case logic as above,
    but applies it to the timestamp field (from which timestamp_start is derived)
    we use the exact same `when` clauses as above so that timestamp & timestamp_start are treated identically
    */
    , case
      -- 1. our timestamp_start from the client is consistent with the server clock
      when watched_records.timestamp_start between playback_sessions.started_at and playback_sessions.ended_at
        then watched_records.timestamp
      /*
      2. our received_at from segment is consistent with the server clock
      then use the received_at time instead of the timestamp from the client
      */
      when watched_records.received_at between playback_sessions.started_at and playback_sessions.ended_at
        then watched_records.received_at
      -- 3. else use playback session if it is known, or rely on the suspect client clock        
      else coalesce(playback_sessions.ended_at, watched_records.timestamp)
    end                            as timestamp_adjusted
  from watched_records
  left join playback_sessions on (watched_records.playback_session_id = playback_sessions.playback_session_id)

)

select
  adjusted_timestamps.watched_minutes_id
  , adjusted_timestamps.user_id
  , adjusted_timestamps.playback_session_id
  , adjusted_timestamps.asset_id
  , adjusted_timestamps.requested_asset_id
  , adjusted_timestamps.played_asset_id
  , adjusted_timestamps.asset_type
  , adjusted_timestamps.platform
  , adjusted_timestamps.channel_id
  , adjusted_timestamps.show_id
  , adjusted_timestamps.episode_id
  , adjusted_timestamps.tms_series_id
  , adjusted_timestamps.philo_series_id
  , adjusted_timestamps.run_time
  , adjusted_timestamps.is_live
  , adjusted_timestamps.timestamp_start_adjusted as timestamp_start
  , adjusted_timestamps.timestamp_adjusted       as "timestamp"
  , adjusted_timestamps.dbt_processed_at
  , adjusted_timestamps.channel_name
  , adjusted_timestamps.channel_callsign
  , adjusted_timestamps.show_title
  , adjusted_timestamps.episode_title
  , adjusted_timestamps.series_title
  , adjusted_timestamps.is_paid_programming
  -- limit watched minutes to be the lesser of the minutes summed or twice the run_time
  -- run_time is stored in seconds
  , case
    when adjusted_timestamps.run_time is null then adjusted_timestamps.minutes -- we don't know the run_time
    else least(adjusted_timestamps.minutes, 2.0 * adjusted_timestamps.run_time / 60.0)
  end                                            as minutes
from adjusted_timestamps
