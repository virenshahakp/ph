with

source as (

  select * from {{ source('web_prod', 'player_state_changed') }}

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
    , {{ normalize_id("_id") }}                     as asset_id
    , sequence_number   as sequence_number
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
  from source
  -- We introduced player_state_changed with Web analytics version 5.
  where environment_analytics_version >= 5
    and sdpid is not null

)

select *
from renamed
{%- if target.name != 'prod' %}
  where received_at >= {{ dbt.dateadd('day', -incremental_dev_mode_days(), 'current_date') }}
{%- endif -%}
