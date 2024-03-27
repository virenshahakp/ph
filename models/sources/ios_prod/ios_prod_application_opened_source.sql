with

source as (

  select * from {{ source('ios_prod', 'application_opened') }}

)

, renamed as (

  select
    id                       as event_id
    , 'ios'                  as context_campaign_source
    , 'philo'                as context_campaign_name
    , 'philo'                as context_campaign_term
    , 'philo'                as context_campaign_medium
    , ''                     as context_page_referrer
    , 'ios'                  as context_user_agent
    , 'ios'                  as context_page_path
    , 'ios'                  as url
    , 'ios'                  as context_campaign_content
    , null                   as context_campaign_content_id
    , "timestamp"            as visited_at
    , 2                      as priority
    , 'ios'                  as visit_type
    , null                   as coupon_code
    , null                   as reference
    , lower(anonymous_id)    as anonymous_id
    , lower(user_id)         as user_id
    , nullif(context_ip, '') as context_ip
  from source
  where anonymous_id is not null

)

select * from renamed
