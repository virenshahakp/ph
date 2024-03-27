with

opened as (

  select * from {{ source('tvos_prod', 'application_opened') }}

)

, renamed as (

  select -- noqa: L034
    id                              as event_id
    , lower(anonymous_id)           as anonymous_id
    , lower(user_id)                as user_id
    , nullif(context_ip, '')        as context_ip
    , 'tvos'                        as context_campaign_source
    , 'philo'                       as context_campaign_name
    , 'philo'                       as context_campaign_term
    , 'philo'                       as context_campaign_medium
    , ''                            as context_page_referrer
    , 'tvos'                        as context_user_agent
    , 'tvos'                        as context_page_path
    , 'tvos'                        as url
    , 'tvos'                        as context_campaign_content
    , null                          as context_campaign_content_id
    , "timestamp"                   as visited_at
    , 2                             as priority
    , 'tvos'                        as visit_type
    , null                          as coupon_code
    , null                          as reference
    , environment_analytics_version as environment_analytics_version
    , lower(context_device_id)      as context_device_id
    , context_app_version           as context_app_version
  from opened
  where anonymous_id is not null

)

select * from renamed
