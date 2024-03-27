with

player_state_changed as (

  select * from {{ source('viziotv_prod', 'player_state_changed') }}

)

, renamed as (

  select
    id                           as event_id
    , sdpid                      as playback_session_id
    , received_at                as received_at
    , "timestamp"                as "timestamp"

    , hashed_session_id          as hashed_session_id
    , context_ip                 as context_ip
    , type                       as type -- noqa: L029

    , action                     as action -- noqa: L029
    , bitrate                    as bitrate
    , uuid_ts                    as loaded_at
    , sequence_number            as sequence_number

    , delay_ms / 1000.0          as delay
    , {{ normalize_id("_id") }}                              as asset_id
    , position_start_ms / 1000.0 as position_start
    , position_stop_ms / 1000.0  as position_stop
    -- in the 2.0 release position_stop_ms got renamed to position_end_ms


    , lower(user_id)             as user_id
    , lower(anonymous_id)        as anonymous_id
  from player_state_changed

)


select
  event_id
  /*
  an explicit cast of user_id was needed in creating the staging table
  as dbt was generating a different size than the rest of the playerStateChanged
  tables on other platforms which will cause the union to error since user_id
  is the distribution key for the existing table
  */
  , user_id::varchar(512) as user_id
  , anonymous_id
  , received_at
  , playback_session_id
  , "timestamp"
  , delay
  , position_start
  , position_stop
  , hashed_session_id
  , context_ip
  , action
  , sequence_number
  , asset_id
  , type
  , bitrate
  , loaded_at
from renamed
{%- if target.name != 'prod' %}
  where received_at >= {{ dbt.dateadd('day', -incremental_dev_mode_days(), 'current_date') }}
{%- endif -%}