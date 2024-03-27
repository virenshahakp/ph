with

source as (

  select * from {{ source('rails_prod', 'pages') }}

)

, renamed as (

  select
    id                                     as event_id
    , anonymous_id                         as anonymous_id
    , user_id                              as user_id
    , "timestamp"                          as visited_at
    , null                                 as coupon_code
    , received_at                          as received_at
    , path                                 as path
    , nullif(context_ip, '')               as context_ip
    , nullif(context_campaign_source, '')  as context_campaign_source
    , nullif(context_campaign_name, '')    as context_campaign_name
    , nullif(context_campaign_term, '')    as context_campaign_term
    , nullif(context_campaign_medium, '')  as context_campaign_medium
    , nullif(context_page_referrer, '')    as context_page_referrer
    , nullif(context_user_agent, '')       as context_user_agent
    , nullif(context_page_path, '')        as context_page_path
    , nullif(context_campaign_content, '') as context_campaign_content
    , nullif(url, '')                      as url
  from source
  where anonymous_id is not null

)

select * from renamed
