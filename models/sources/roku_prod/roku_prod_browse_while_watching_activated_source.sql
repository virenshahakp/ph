with

browse_while_watching_activated as (

  select * from {{ source('roku_prod', 'browse_while_watching_activated') }}

)

, renamed as (
  select
    id                             as event_id
    , user_id                      as user_id
    , received_at                  as received_at
    , position_ms                  as position_ms
    , original_timestamp           as original_timestamp
    , anonymous_id                 as anonymous_id
    , environment_analyticsversion as environment_analytics_version
    , "event"                      as "event" -- noqa: L059
    , environment_screen_width     as environment_screen_width
    , environment_screen_height    as environment_screen_height
    , "timestamp"                  as "timestamp"
    , uuid_ts                      as loaded_at
    , sdpid                        as playback_session_id
    , sent_at                      as sent_at
    , {{ normalize_id("_id") }}                                as asset_id
    , trigger                      as trigger -- noqa: L029
    , hashed_session_id            as hashed_session_id
    , type                         as asset_type



  from browse_while_watching_activated
  where environment_analytics_version >= 7
    and sdpid is not null

)

select
  event_id
  , user_id
  , received_at
  , position_ms
  , original_timestamp
  , anonymous_id
  , environment_analytics_version
  , event
  , environment_screen_width
  , environment_screen_height
  , "timestamp"
  , loaded_at
  , playback_session_id
  , sent_at
  , asset_id
  , trigger
  , hashed_session_id
  , asset_type
from renamed
{%- if target.name != 'prod' %}
  where received_at >= {{ dbt.dateadd('day', -incremental_dev_mode_days(), 'current_date') }}
{%- endif -%}
 

