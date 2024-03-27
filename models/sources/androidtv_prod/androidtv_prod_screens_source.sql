with

screens as (

  select * from {{ source('androidtv_prod', 'screens') }}

)

, renamed as (

  select
    id                  as event_id
    , user_id           as user_id
    , anonymous_id      as anonymous_id
    , context_device_id as device_id
    -- , hashed_session_id                                     as hashed_session_id
    , "timestamp"       as event_timestamp
    , received_at       as received_at
    , uuid_ts           as loaded_at
    , coalesce(
      environment_analytics_version
      , context_environment_analytics_version
    )                   as environment_analytics_version
    , coalesce(
      context_environment_app_version
      , environment_app_version
      , context_app_version
    )                   as app_version
    , case when name = 'mytv' then 'home'
      when name = 'channelDetail' then 'channel_detail'
      when name = 'showDetail' then 'show_detail'
      when name = 'presentationDetail' then 'presentation_detail'
      when name = 'playerOptions' then 'player_options'
      when name = 'playerOverlay' then 'player_overlay'
      when name = 'playbackError' then 'playback_error'
      when name like 'Player:%' then 'player'
      when name = 'Philo' then null -- these were the automatic analytics events that Segment had been firing since a long time. We disabled them starting app release version 6.12.x and analytics version 22 
      else lower(name)
    end                 as screen_name
  from screens

)

select *
from renamed
{%- if target.name != 'prod' %}
  where received_at >= {{ dbt.dateadd('day', -incremental_dev_mode_days(), 'current_date') }}
{%- endif -%}
