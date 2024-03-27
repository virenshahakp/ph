with

player_state_changed as (

  select * from {{ source('roku_prod', 'player_state_changed') }}

)

, renamed as (

  select
    id                  as event_id
    , user_id           as user_id
    , sdpid             as playback_session_id
    , received_at       as received_at
    , "timestamp"       as "timestamp"
    , hashed_session_id as hashed_session_id
    , context_ip        as context_ip

    , action            as action
    , bitrate           as bitrate
    , uuid_ts           as loaded_at

    , sequence_number   as sequence_number
    , coalesce(
      delay_ms / 1000.0
      , delay
    )                   as delay
    , coalesce(
      position_start_ms / 1000.0
      , position_start
    )                   as position_start
    , {{ normalize_id("_id") }}                     as asset_id
    , coalesce(
      position_stop_ms / 1000.0
      , position_stop
    )                   as position_stop
  from player_state_changed
  -- We introduced player_state_changed with Roku analytics version 7.
  -- Coincidently, the android code base also added player_state_changed in
  -- Android analytics version 7.
  where environment_analyticsversion >= 7

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
  , action
  , sequence_number
  , asset_id
  , bitrate
  , loaded_at
from renamed
{%- if target.name != 'prod' %}
  where received_at >= {{ dbt.dateadd('day', -incremental_dev_mode_days(), 'current_date') }}
{%- endif -%}