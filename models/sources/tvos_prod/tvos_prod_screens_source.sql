with

screens as (

  select * from {{ source('tvos_prod', 'screens') }}

)

, renamed as (

  select
    id                              as event_id
    , "timestamp"                   as visited_at
    , received_at                   as received_at
    , uuid_ts                       as loaded_at
    , environment_analytics_version as environment_analytics_version
    , context_app_version           as app_version
    , lower(anonymous_id)           as anonymous_id
    , lower(user_id)                as user_id
    , lower(context_device_id)      as context_device_id
    , case when name = 'channelDetail' then 'channel_detail'
      when name = 'showDetail' then 'show_detail'
      when name = 'presentationDetail' then 'presentation_detail'
      when name = 'playerOptions' then 'player_options'
      when name = 'playerOverlay' then 'player_overlay'
      when name = 'playbackError' then 'playback_error'
      -- Screen names such as Philo_tvOS.TVHome or Philo_tvOS.TVGuide were used before the names were standardized.
      when name like 'Philo_tvOS.TV%' then lower(split_part(name, 'Philo_tvOS.TV', 2))
      -- duplicate  Philo_tvOS.TVSavedShows screen event fires in addition to a saved screen event
      when name = 'Philo_tvOS.TVSavedShows' then null
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
