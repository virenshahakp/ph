with

player_state_changed as (

  select * from {{ source('ios_prod', 'player_state_changed') }}

)

, renamed as (

  select
    id                  as event_id
    , sdpid             as playback_session_id
    , received_at       as received_at
    , "timestamp"       as "timestamp"
    , hashed_session_id as hashed_session_id
    , context_ip        as context_ip
    , sequence_number   as sequence_number
    , action            as action
    , bitrate           as bitrate
    , uuid_ts           as loaded_at

    , coalesce(
      delay_ms / 1000.0
      , delay
    )                   as delay
    , coalesce(
      position_start_ms / 1000.0
      , position_start
    )                   as position_start
    , coalesce(
      position_stop_ms / 1000.0
      , position_stop
    )                   as position_stop
    , lower(user_id)    as user_id
    , {{ normalize_id("_id") }}                     as asset_id
  from player_state_changed
  -- We introduced player_state_changed with Apple analytics version 12.
  where environment_analytics_version >= 12
    and sdpid is not null

)


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
  , sequence_number
  , action
  , asset_id
  , bitrate
  , loaded_at
from renamed
{%- if target.name != 'prod' %}
  where received_at >= {{ dbt.dateadd('day', -incremental_dev_mode_days(), 'current_date') }}
{%- endif -%}