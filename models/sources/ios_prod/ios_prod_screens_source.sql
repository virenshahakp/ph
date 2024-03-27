with

screens as (

  select * from {{ source('ios_prod', 'screens') }}

)

, renamed as (

  select
    id                              as event_id
    , "timestamp"                   as visited_at
    , received_at                   as received_at
    , uuid_ts                       as loaded_at
    , environment_analytics_version as environment_analytics_version
    , context_app_version           as app_version
    , lower(user_id)                as user_id
    , lower(anonymous_id)           as anonymous_id
    , lower(context_device_id)      as context_device_id
    , case when name = 'mytv' then 'home'
      when name = 'channelDetail' then 'channel_detail'
      when name = 'showDetail' then 'show_detail'
      when name = 'presentationDetail' then 'presentation_detail'
      when name = 'playerOptions' then 'player_options'
      when name = 'playerOverlay' then 'player_overlay'
      when name = 'playbackError' then 'playback_error'
      when name like 'Philo.%' then lower(split_part(name, '.', 2))
      else lower(name)
    end                             as screen_name
  from screens
  where anonymous_id is not null

)

select *
from renamed

{%- if target.name != 'prod' %}
  where received_at >= {{ dbt.dateadd('day', -incremental_dev_mode_days(), 'current_date') }}
{%- endif -%}
