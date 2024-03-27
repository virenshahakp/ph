with

source as (

  select * from {{ source('android_prod', 'application_installed') }}

)

, renamed as (

  select
    id                              as event_id
    , anonymous_id                  as anonymous_id
    , user_id                       as user_id
    , 'philo'                       as context_campaign_name
    , 'philo'                       as context_campaign_term
    , 'philo'                       as context_campaign_medium
    , ''                            as context_page_referrer
    , 'android'                     as context_user_agent
    , 'android'                     as context_page_path
    , 'android'                     as url
    , 'android'                     as context_campaign_content
    , null                          as context_campaign_content_id
    , "timestamp"                   as visited_at
    , 1                             as priority
    , 'android'                     as visit_type
    , null                          as coupon_code
    , context_device_advertising_id as context_device_advertising_id
    , null                          as reference
    , nullif(context_ip, '')        as context_ip
  from source
  where anonymous_id is not null

)

select * from renamed
