with

installed as (

  select * from {{ source('ios_prod', 'application_installed') }}

)

, renamed as (

  select -- noqa: L034
    id                              as event_id
    , lower(anonymous_id)           as anonymous_id
    , lower(user_id)                as user_id
    , nullif(context_ip, '')        as context_ip
    , 'philo'                       as context_campaign_name
    , 'philo'                       as context_campaign_term
    , 'philo'                       as context_campaign_medium
    , ''                            as context_page_referrer
    , 'ios'                         as context_user_agent
    , 'ios'                         as context_page_path
    , 'ios'                         as url
    , 'ios'                         as context_campaign_content
    , null                          as context_campaign_content_id
    , "timestamp"                   as visited_at
    , 1                             as priority
    , 'iOS App Store'               as visit_type
    , null                          as coupon_code
    , lower(context_device_id)      as context_device_id
    , context_device_advertising_id as context_device_advertising_id
    , null                          as reference
    , environment_analytics_version as environment_analytics_version
    , context_app_version           as context_app_version

  from installed
  where
    anonymous_id is not null
    -- LB: ios had a bug where application installed was fired when the user_id
    -- had already been sent.  This is not a real install.
    and user_id is null
)

select * from renamed
