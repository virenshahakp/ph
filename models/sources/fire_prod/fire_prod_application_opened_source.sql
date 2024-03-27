with

source as (

  select * from {{ source('fire_prod', 'application_opened') }}

)

, renamed as (

  select
    id                       as event_id
    , anonymous_id           as anonymous_id
    , user_id                as user_id
    , 'fire'                 as context_campaign_source
    , 'philo'                as context_campaign_name
    , 'philo'                as context_campaign_term
    , 'philo'                as context_campaign_medium
    , ''                     as context_page_referrer
    , 'fire'                 as context_user_agent
    , 'fire'                 as context_page_path
    , 'fire'                 as url
    , 'fire'                 as context_campaign_content
    , null                   as context_campaign_content_id
    , "timestamp"            as visited_at
    , 2                      as priority
    , 'fire'                 as visit_type
    , null                   as coupon_code
    , null                   as reference
    , nullif(context_ip, '') as context_ip
  from source
  where anonymous_id is not null

)

select * from renamed
