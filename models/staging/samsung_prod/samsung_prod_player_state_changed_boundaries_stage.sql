{{
  config(
    materialized='incremental'
    , unique_key='event_id'
    , dist='playback_session_id'
    , sort=['playback_session_id', 'dbt_processed_at', 'timestamp']
  )
}}

/*
  This model processes the player state changed events in incrementally delivered
  batches. The batch of data may contain data that changes our understanding of
  previously delivered batches due to out of order data or continuation of playback
  that was occurring at the time of the last batch processing.

  We stage it by playback_session_id to be able to combine the new and existing data
  about a given playback_session. The unique_key allows us to replace the specific events
  when they are recalculated due to new data.
*/ 

{%- set max_processed_at = incremental_max_value('dbt_processed_at') %}

with

playback_sessions_to_process as (

  select playback_session_id
  from {{ ref('samsung_prod_player_state_changed_stage') }}
  {%- if is_incremental() %}
    where dbt_processed_at > {{ max_processed_at }}
  {%- endif %}

)

, events_to_process as (

  select
    samsung_prod_player_state_changed_stage.event_id
    , samsung_prod_player_state_changed_stage.user_id
    , samsung_prod_player_state_changed_stage.playback_session_id
    , samsung_prod_player_state_changed_stage.received_at
    , samsung_prod_player_state_changed_stage."timestamp"
    , samsung_prod_player_state_changed_stage.delay
    , samsung_prod_player_state_changed_stage.position_start
    , samsung_prod_player_state_changed_stage.position_stop
    , samsung_prod_player_state_changed_stage.hashed_session_id
    , samsung_prod_player_state_changed_stage.context_ip
    , samsung_prod_player_state_changed_stage.action
    , samsung_prod_player_state_changed_stage.bitrate
    , samsung_prod_player_state_changed_stage.loaded_at
    /*
    We have two methods of determining what order the events arrived in.
    1) historically by sorting timestamp, action, loaded_at, and event_id to get a deterministic sort.
       This method accurately sorts the starting events, but may not get an accurate sort on tied timestamps
       during a playback session.
    2) sequence_number which was added in August 2023.

    The vast majority of the time ordering by timestamp is sufficient, but we add "action" into this
    for a few cases where the first 2+ events come in with the exact same client "timestamp"
    adding "action" into the order by puts start before stop or sync which is conveniently the order
    we would want them in in case of a tie.
    We add the row_number so that we can use that for consistent sorting for the rest of our
    event processing.
    Without this logic a small number of events get a null starting timestamp for the watched range because
    there is no guarantee that window functions will break the tie in the same way even within the same CTE
    */
    , sequence_number
    , row_number() over (partition by playback_session_id order by "timestamp", action, loaded_at, event_id) as rn
    /*
    Our client applications can request an asset with additional decoration to the
    presentation id as part of their request. Our systems expect the presentation id
    to be base64 encoded when being passed between different pieces of software.

    On the analytics side, and in particular when processing player state changed events,
    we are focused on the first two elements in the unencoded string that identifies
    the content (aka asset) that is being requested.

    For example these two base64 encoded values are for the same content:
    Vk9EOjYwODU0ODg5OTY0ODcwNDA5MjpzaGFya3dlZWs=
    Vk9EOjYwODU0ODg5OTY0ODcwNDA5Mg==

    The unencoded values:
    VOD:608548899648704092:sharkweek
    VOD:608548899648704092

    For analytics purposes the first (longer) id value will never match the VOD id
    that is in our guide metadata tables.

    The following case statement parses the unencoded string and re-encodes it with
    only the first two elements.

    We cannot do this in one step in the preceding model because redshift does not allow
    nesting of user defined functions (the f_base64encode and f_base64decode functions).
    Also python UDFs are slow and we need to be cautious in where we use them.
    */
    , case
      when regexp_count(samsung_prod_player_state_changed_stage.decoded_asset_id, ':') > 1
        then f_base64encode(
            split_part(
              samsung_prod_player_state_changed_stage.decoded_asset_id, ':', 1
            ) || ':'
            || split_part(
              samsung_prod_player_state_changed_stage.decoded_asset_id, ':', 2
            )
          )
      else samsung_prod_player_state_changed_stage.asset_id
    end                                                                                                      as asset_id
  from {{ ref('samsung_prod_player_state_changed_stage') }}
  where playback_session_id in (
      select playback_session_id from playback_sessions_to_process
    )
    -- ignore ad events when processing watched ranges
    and action in ('start', 'stop', 'sync')

)

, generate_lead_and_lag_events as (

  select
    *
    , lag(action, 1) over (partition by playback_session_id order by coalesce(sequence_number, rn))       as previous_action
    , lag("timestamp", 1) over (partition by playback_session_id order by coalesce(sequence_number, rn))  as previous_timestamp
    , lead(action, 1) over (partition by playback_session_id order by coalesce(sequence_number, rn))      as next_action
    , lead("timestamp", 1) over (partition by playback_session_id order by coalesce(sequence_number, rn)) as next_timestamp
  from events_to_process

)

-- determine start, end, boundary events
, identify_boundaries as (

  select
    generate_lead_and_lag_events.*

    -- consider putting these into a macro for re-use across platforms
    , case
      -- starts are always a starting boundary
      when action = 'start'
        then true
      -- first events are always a starting boundary, row_number starts at 1, sequence_number starts at 0, this lets us apply this in either scenario
      when previous_action is null or coalesce(sequence_number + 1, rn) = 1
        then true
      -- sync events immediately following a stop are a starting boundary, but also an incomplete data set
      when action = 'sync' and previous_action = 'stop'
        then true
      else false
    end as is_starting_event
    , case
      -- stops are always an ending boundary
      when action = 'stop'
        then true
      -- last events are always an ending boundary
      when next_action is null
        then true
      -- consecutive starts are also a boundary, but also an incomplete data set
      when action = 'start' and previous_action = 'start'
        then true
      else false
    end as is_ending_event

  from generate_lead_and_lag_events

)

, generate_range_start_end_timestamps as (

  select
    *
    , case when is_starting_event is true then "timestamp" end as range_start_at
    , case when is_ending_event is true then "timestamp" end   as range_end_at
  from identify_boundaries

)

, fill_in_watched_range_timestamps as (

  select
    event_id
    , user_id
    , playback_session_id
    , received_at
    , "timestamp"
    , delay
    , position_start
    , position_stop
    , hashed_session_id
    , context_ip
    , action
    , asset_id
    , bitrate
    , is_starting_event
    , is_ending_event
    , previous_action
    , previous_timestamp
    , next_action
    , next_timestamp
    , loaded_at
    , case when is_starting_event is false
        then lag(position_start, 1) over (partition by playback_session_id order by coalesce(sequence_number, rn))
    end as previous_position_start
    , case when is_starting_event is false
        then lag(position_stop, 1) over (partition by playback_session_id order by coalesce(sequence_number, rn))
    end as previous_position_stop
    , last_value(
      range_start_at ignore nulls
    ) over (
      partition by playback_session_id order by coalesce(sequence_number, rn) rows unbounded preceding
    )   as watched_range_start_at
    , first_value(
      range_end_at ignore nulls
    ) over (
      partition by playback_session_id order by coalesce(sequence_number, rn) rows between current row and unbounded following
    )   as watched_range_end_at
  from generate_range_start_end_timestamps

)

, validations as (

  select
    event_id
    , user_id
    , playback_session_id
    , received_at
    , "timestamp"
    , delay
    , position_start
    , position_stop
    , hashed_session_id
    , context_ip
    , action
    , asset_id
    , bitrate
    , is_starting_event
    , is_ending_event
    , previous_action
    , previous_timestamp
    , next_action
    , next_timestamp
    , watched_range_start_at
    , watched_range_end_at
    , loaded_at
    /*
    if the ending position is before the starting position
    or the ending timestamp is before the starting timestamp
    we do not want to use those specific values, if both are
    null then we will not counting any minutes viewed
    */
    , case
      when position_stop >= position_start
        then position_stop - position_start
    end       as position_duration_seconds
    , case
      when watched_range_end_at >= watched_range_start_at
        then datediff(
            'millisecond', watched_range_start_at, watched_range_end_at
          ) / 1000.0
    end       as watched_range_timestamp_duration_seconds


    /*
    Events for validation and monitoring
    We should get a sync every 300 seconds, so when working correctly
    there shouldn't be any gaps larger than that (with a slight buffer for clock skew).

    If position stop jumps by more than 300 the same issue is present.
    There was a period of time where some sync values were being deleted :( from
    our player state change tables, so this is likely to vary over time and by platform.

    If position start jumps by any amount that will invalidate our final range calculation
    if we need to rely upon positions. So if this is happening we could choose to rely only
    on clock deltas instead of position deltas.
    */

    , coalesce(
      is_starting_event is false
      and datediff('seconds', previous_timestamp, "timestamp") > 310, false
    )         as has_reporting_delay
    , coalesce(
      is_starting_event is false
      and position_start - previous_position_start > 10, false
    )         as has_position_start_jump
    , coalesce(
      is_starting_event is false
      and position_stop - previous_position_stop > 310, false
    )         as has_position_stop_jump

    -- generate the processed at time, potentially an update for an existing event
    , sysdate as dbt_processed_at
  from fill_in_watched_range_timestamps

)

select
  validations.*
  , count(
    case when has_reporting_delay is true then 1 end
  ) over (partition by playback_session_id, asset_id)          as played_asset_reporting_delay_count
  , count(
    case when has_position_start_jump is true then 1 end
  ) over (partition by playback_session_id, asset_id)          as played_asset_position_start_jump_count
  , count(
    case when has_position_stop_jump is true then 1 end
  ) over (partition by playback_session_id, asset_id)          as played_asset_position_stop_jump_count
  , count(1) over (partition by playback_session_id, asset_id) as played_asset_total_event_count
from validations