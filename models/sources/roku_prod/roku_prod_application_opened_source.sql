with

source as (

  select * from {{ source('roku_prod', 'application_opened') }}

)

, renamed as (

  select
    id                       as event_id
    , anonymous_id           as anonymous_id
    , user_id                as user_id
    , uuid_ts                as loaded_at
    , null                   as source
    , 'roku'                 as url
    , "timestamp"            as visited_at
    , null                   as coupon_code
    , null                   as reference
    {# swap out two lines below after the FAST release; contentId, assetType, utm parameters will be added -EM
    , {{ normalize_id("content_id") }}         as context_campaign_content_id #}
    , null                   as context_campaign_content_id
    , ''                     as context_page_referrer
    , 'roku'                 as context_user_agent
    , 'roku'                 as context_page_path
    , null                   as context_campaign_source
    , null                   as context_campaign_name
    , null                   as context_campaign_term
    , null                   as context_campaign_medium
    , null                   as context_campaign_content
    -- swap out two lines below after the FAST release -EM
    --, coalesce(fast_account_request, false) as has_fast_account_request
    , false                  as has_fast_account_request
    , nullif(context_ip, '') as context_ip
  from source
  where anonymous_id is not null

)


select * from renamed
{%- if target.name != 'prod' %}
  where loaded_at >= {{ dbt.dateadd('day', -incremental_dev_mode_days(), 'current_date') }}
{%- endif -%}
