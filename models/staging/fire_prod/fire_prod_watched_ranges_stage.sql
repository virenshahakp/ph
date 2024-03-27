{{
  config(
    materialized='view'
  )
}}

with 

-- for calculation of watched ranges we only need the ending event
player_ending_ranges as (

  select *
  from {{ ref('fire_prod_player_state_changed_boundaries_stage') }}
  where is_ending_event is true

)

select
  event_id
  , user_id
  , playback_session_id
  , asset_id
  , received_at
  , watched_range_start_at as timestamp_start
  , watched_range_end_at as "timestamp"
  , delay
  , position_start
  , position_stop
  , hashed_session_id
  , context_ip
  , action
  , bitrate
  , position_duration_seconds
  , watched_range_timestamp_duration_seconds
  , loaded_at
  , dbt_processed_at  
  , least(position_duration_seconds, watched_range_timestamp_duration_seconds) as watched_seconds
from player_ending_ranges