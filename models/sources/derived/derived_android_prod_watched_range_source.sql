with

source as (

  select * from {{ source('derived', 'android_prod_watched_range') }}

)

/*
  Derived watched range roll up consecutive watched seconds and use the first
  watched second event as the template for the watched_range event. This means
  timestamp and received_at are from the start of the range instead of the
  end. This is different from most of our other events because the two
  timestamps are normally taken right when the event is sent or just as it is
  received at the server. timestamp_end and position_end are the values from
  the last watched second. Here we remap the columns to make the two timestamp
  names match other events. Unfortunately, we only have a received_at for the
  first watched second. We could diff client timestamp_start and timestamp and
  add it to received_at but I worry that funkiness in timestamps from the
  client will impact the reliability of received_at which is generated at
  Segment's server and is critical to incremental data updates. Use received_at
  as is as a compromise.
*/

, renamed as (

  select
    id                              as event_id
    , user_id                       as user_id
    , sdpid                         as playback_session_id
    , received_at                   as received_at
    , "timestamp"                   as timestamp_start
    , timestamp_end                 as "timestamp"
    , delay                         as delay
    , position                      as position_start
    , position_end                  as position_stop
    , context_ip                    as context_ip
    , 'android'                     as platform
    , uuid_ts                       as loaded_at
    , environment_hashed_session_id as hashed_session_id
    , {{ normalize_id("_id") }}                                 as asset_id
  from source
  -- Prior to Android analytics version 7 we used watched_seconds event which
  -- we convert to this watched_range event. With version 7 we switched to
  -- player_state_changed event.
  where (environment_analytics_version is null or environment_analytics_version < 7)
    and user_id is not null
    and asset_id is not null

)

select *
from renamed
{%- if target.name != 'prod' %}
  where received_at >= {{ dbt.dateadd('day', -incremental_dev_mode_days(), 'current_date') }}
{%- endif -%}
