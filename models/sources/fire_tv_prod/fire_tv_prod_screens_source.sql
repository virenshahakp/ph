with

source as (

  select * from {{ source('fire_tv_prod', 'screens') }}

)

, renamed as (

  select
    id                       as event_id
    , anonymous_id           as anonymous_id
    , user_id                as user_id
    , 'firetv'               as context_campaign_source
    , 'philo'                as context_campaign_name
    , 'philo'                as context_campaign_term
    , 'philo'                as context_campaign_medium
    , ''                     as context_page_referrer
    , 'firetv'               as context_user_agent
    , 'firetv'               as context_page_path
    , 'firetv'               as url
    , 'firetv'               as context_campaign_content
    , null                   as context_campaign_content_id
    , "timestamp"            as visited_at
    , received_at            as received_at
    , uuid_ts                as loaded_at
    , 2                      as priority
    , 'firetv'               as visit_type
    , null                   as coupon_code
    , name                   as name
    , null                   as reference
    , coalesce(
      context_environment_app_version
      , environment_app_version
      , context_app_version
    )                        as app_version
    , case when name = 'channelDetail' then 'channel_detail'
      when name = 'showDetail' then 'show_detail'
      when name = 'presentationDetail' then 'presentation_detail'
      when name = 'playerOptions' then 'player_options'
      when name = 'playerOverlay' then 'player_overlay'
      when name = 'playbackError' then 'playback_error'
      when name = 'mytv' then 'home'
      when name like 'Player:%' then 'player'
      else lower(name)
    end                      as screen_name
    , nullif(context_ip, '') as context_ip
  from source
  where anonymous_id is not null

)

select * from renamed
{%- if target.name != 'prod' %}
  where received_at >= {{ dbt.dateadd('day', -incremental_dev_mode_days(), 'current_date') }}
{%- endif -%}