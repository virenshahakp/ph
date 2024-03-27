{% macro watched_minutes_generate_sql(is_historic) %}

/*
  We want to maintain the condition that there is only ever one watched minutes
  record for each user (user_id) watching an asset (asset_id) in one sitting
  (playback_session_id, platform). This allows us to more efficiently count,
  using COUNT(1), records to get the number of playbacks. We create a
  surrogate_key called id with those columns to enforce this constraint.

  Incremental update with unique_key=id will ensure there is only one record in
  the table by overwriting existing records with a new version from the
  incremental update. To avoid loosing already watched minutes in the
  table, we need to merge watched minutes records that have already been
  created with any new watched_ranges for the same record. We do this by
  loading watched ranges since the last incremental update and using the id
  from these ranges to find the existing watched minutes that need to get
  updated (existing_wm_to_update). We then transform the existing
  watched_minutes to look like watched ranges so that we can union (wr_unioned)
  the two data sets and aggregate them into a watched_minutes record (wm) .
*/

-- Days to lookback when finding existing watched minutes to update
{%- set lookback_days = 14 %}

{%- set this_max_received_at = incremental_max_value("received_at") %}

with

watched_ranges as (

  select * from {{ ref('fct_watched_ranges') }}

)

, meta as (

  select * from {{ ref('dim_guide_metadata') }}

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
    , max(ended_at) as ended_at
  from {{ ref('fct_playback_sessions') }}
  {{ dbt_utils.group_by(n=1) }}

)

, wr_incremental as (

  select 
    {{ dbt_utils.generate_surrogate_key(['user_id', 'playback_session_id', 'requested_asset_id', 'played_asset_id', 'platform']) }} as id
    , user_id
    , playback_session_id
    , requested_asset_id as asset_id --legacy column support
    , requested_asset_id
    , played_asset_id
    , platform
    , channel_id
    , show_id 
    , episode_id
    , run_time
    , tms_series_id
    , philo_series_id
    , case
        when delay is null
        then 0
        else 1
      end as delay_count
    , (position_stop - position_start) / 60.0 as minutes
    , timestamp_start
    , "timestamp"
    , received_at
  from watched_ranges
  where
    position_stop > position_start
    {%- if is_historic %}
      and received_at < {{ dbt.dateadd('day', -incremental_recent_days(), 'getdate()') }}
      {%- if is_incremental() %}
        and received_at > {{ this_max_received_at }}
      {%- endif %}
    {%- else %}
      and received_at >= {{ dbt.dateadd('day', -incremental_recent_days(), 'getdate()') }}
    {%- endif %}

)

{%- if is_incremental() %}
, existing_wm_to_update as (

  select 
    id
    , user_id
    , playback_session_id
    , asset_id --legacy column support
    , requested_asset_id
    , played_asset_id
    , platform
    , channel_id
    , show_id
    , episode_id
    , run_time
    , tms_series_id
    , philo_series_id
    , case
        when is_live 
        then 1
        else 0
      end as delay_count
    , minutes
    , timestamp_start
    , "timestamp"
    , received_at
  from {{ this }} as this
  where received_at > {{ dbt.dateadd('day', -lookback_days, this_max_received_at) }}
    -- only update existing watched minutes if there are incremental watched
    -- ranges that will update the watched minutes record
    and exists (
      select 1
      from wr_incremental
      where wr_incremental.id = this.id
    )

)

-- Define columns to select once to ensure same order in union all
{%- set wr_select_columns -%}
  id
  , user_id
  , playback_session_id
  , asset_id --legacy column support
  , requested_asset_id
  , played_asset_id
  , platform
  , channel_id
  , show_id 
  , episode_id
  , run_time
  , tms_series_id
  , philo_series_id
  , delay_count
  , minutes
  , timestamp_start
  , "timestamp"
  , received_at
{% endset %}

, wr_unioned as (

  -- add a wr type to indicate whether the record is exisiting data or new/incremental
  select 
    {{ wr_select_columns }}
    , 'incremental'::text as wr_type
  from wr_incremental
  union all
  select 
    {{ wr_select_columns }}
    , 'existing'::text as wr_type
  from existing_wm_to_update

)

{%- else%}

, wr_unioned as (

  -- all records in a recent run are wr type new/incremental
  select *, 'incremental'::text as wr_type from wr_incremental

)

{%- endif %}

, wm as (

  select
    id
    , user_id
    , playback_session_id
    , asset_id --legacy column support
    , requested_asset_id
    , played_asset_id
    , platform
    -- count non-null delays; on ios/tvos platforms, delay shifts from null to non-null in one session
    , sum(delay_count) > 0 as is_live
    , sum(minutes) as minutes
    , min(timestamp_start) as timestamp_start
    , max(timestamp) as timestamp
    , max(received_at) as received_at
  from wr_unioned
  {{ dbt_utils.group_by(n=7) }}

)

, wm_attributes as (

  -- use the most recent data for the asset attributes in the insert.
  -- these will always come from the incremental events, updating the 
  -- historic data if it exists with the latest guide data
  select
    wr_unioned.id
    , wr_unioned.channel_id
    , wr_unioned.show_id 
    , wr_unioned.episode_id
    , wr_unioned.run_time
    , wr_unioned.tms_series_id
    , wr_unioned.philo_series_id
    , guide_channels.channel_name
    , guide_channels.channel_callsign
    , guide_shows.show_title
    , guide_episodes.episode_title
    , guide_series.series_title
    -- if there is no joined data, default to false
    , coalesce(guide_shows.is_paid_programming, false) as is_paid_programming
  from wr_unioned  
  left join guide_channels on (
    wr_unioned.channel_id = guide_channels.channel_id
    )
  left join guide_shows on (
    wr_unioned.show_id = guide_shows.show_id
    )
  left join guide_episodes on (
    wr_unioned.episode_id = guide_episodes.episode_id
    )
  left join guide_series on (
    wr_unioned.tms_series_id = guide_series.tms_series_id
    and wr_unioned.philo_series_id = guide_series.philo_series_id
    )
  where wr_type = 'incremental'

)

-- It may be possible for records in saved_shows to overlap and a simple left
-- join will double counts watched ranges. Defend against this by counting the
-- number of overlapping saved_shows records.
, wm_with_saved_shows as (

  select
    wm.id
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
    , meta.asset_type
    , wm_attributes.channel_id
    , wm_attributes.show_id
    , wm_attributes.episode_id
    , wm_attributes.run_time
    , wm_attributes.tms_series_id
    , wm_attributes.philo_series_id
    , wm_attributes.channel_name
    , wm_attributes.channel_callsign
    , wm_attributes.show_title
    , wm_attributes.is_paid_programming
    , wm_attributes.episode_title
    , wm_attributes.series_title          
    , count(saved_shows.user_id) as saved_shows_count
  from wm
  join meta on (meta.asset_id = wm.asset_id)
  left join wm_attributes on (wm.id = wm_attributes.id)
  left join saved_shows on (
    saved_shows.user_id = wm.user_id
    and saved_shows.show_id = meta.show_id
    and wm.timestamp between saved_shows.save_at and coalesce(saved_shows.unsave_at, getdate())
  )
  {{ dbt_utils.group_by(n=25) }}

)

, watched_records as (

  select
    id
    , user_id
    , playback_session_id
    , platform
    , asset_id --legacy column support
    , requested_asset_id
    , played_asset_id
    , case
        -- a recording is a lookback when a user has not saved the show
        when asset_type = 'RECORDING' and saved_shows_count = 0 
        then 'LOOKBACK'
        else asset_type
      end as asset_type
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
        then greatest(playback_sessions.started_at, {{ dbt.dateadd('minute', '-1*minutes::INT', 'received_at') }}) -- noqa: L019
      -- 3. else use playback session if it is known, or rely on the suspect client clock
      else coalesce(playback_sessions.started_at, watched_records.timestamp_start)
    end as timestamp_start_adjusted

    /*
    this uses the same case logic as above, but applies it to the timestamp field (from which timestamp_start is derived)
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
    end as timestamp_adjusted
  from watched_records
  left join playback_sessions on (watched_records.playback_session_id = playback_sessions.playback_session_id)

)

select 
  adjusted_timestamps.id
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
  , adjusted_timestamps.minutes
  , adjusted_timestamps.timestamp_start_adjusted as timestamp_start
  , adjusted_timestamps.timestamp_adjusted as "timestamp"
  , adjusted_timestamps.received_at
  , adjusted_timestamps.channel_name
  , adjusted_timestamps.channel_callsign
  , adjusted_timestamps.show_title
  , adjusted_timestamps.episode_title
  , adjusted_timestamps.series_title
  , adjusted_timestamps.is_paid_programming
from adjusted_timestamps

{% endmacro %}
