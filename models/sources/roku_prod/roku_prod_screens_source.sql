with

screens as (

  select * from {{ source('roku_prod', 'screens') }}

)

, renamed as (

  select
    id                             as event_id
    , user_id                      as user_id
    , anonymous_id                 as anonymous_id
    , context_device_id            as device_id
    , hashed_session_id            as hashed_session_id
    , isbww                        as is_bww
    , "timestamp"                  as event_timestamp
    , received_at                  as received_at
    , uuid_ts                      as loaded_at
    , environment_analyticsversion as environment_analytics_version
    , environment_version          as app_version
    , case when name = 'mytv' then 'home'
      when name = 'channelDetail' then 'channel_detail'
      when name = 'showDetail' then 'show_detail'
      when name = 'presentationDetail' then 'presentation_detail'
      when name = 'GuideSchedule' then 'guide'
      when name = 'GuideTop' then 'guide'
      when name = 'Guide-schedule' then 'guide'
      else lower(name)
    end                            as screen_name
  from screens

)

select *
from renamed

{%- if target.name != 'prod' %}
  where received_at >= {{ dbt.dateadd('day', -incremental_dev_mode_days(), 'current_date') }}
{%- endif -%}
