with

source as (

  select * from {{ source('derived', 'web_prod_watched_range') }}

)

-- See derived_android_prod_watched_range_source.sql for timestamp and
-- received_at remapping.

, renamed as (

  select
    id                                     as event_id
    , user_id                              as user_id
    , sdpid                                as playback_session_id
    , received_at                          as received_at
    , "timestamp"                          as timestamp_start
    -- timestamp_end missing in some source rows, substitute with "timestamp"
    , delay                                as delay
    , position                             as position_start
    , position_end                         as position_stop
    , hashed_session_id                    as hashed_session_id
    , context_ip                           as context_ip
    , 'web'                                as platform
    , uuid_ts                              as loaded_at
    , coalesce(timestamp_end, "timestamp") as "timestamp"
    , {{ normalize_id("_id") }}                                        as asset_id
  from source
  -- Prior to Web analytics version 7 we used watched_seconds event which
  -- we convert to this watched_range event. With version 7 we switched to
  -- player_state_changed event.
  where (environment_analytics_version is null or environment_analytics_version < 5)
    and user_id is not null
    and asset_id is not null

)

select *
from renamed
{%- if target.name != 'prod' %}
  where received_at >= {{ dbt.dateadd('day', -incremental_dev_mode_days(), 'current_date') }}
{%- endif -%}
