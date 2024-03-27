with

source as (

  select * from {{ source('web_prod', 'pages') }}

)

, renamed as (

  select
    id                                              as event_id
    , anonymous_id                                  as anonymous_id
    , user_id                                       as user_id
    , "timestamp"                                   as visited_at
    , coupon_code                                   as coupon_code
    , received_at                                   as received_at
    , uuid_ts                                       as loaded_at
    , environment_version                           as app_version
    , nullif(trim(context_ip), '')                  as context_ip
    , nullif(trim(context_campaign_source), '')     as context_campaign_source
    , nullif(trim(context_campaign_name), '')       as context_campaign_name
    , nullif(trim(context_campaign_term), '')       as context_campaign_term
    , nullif(trim(context_campaign_medium), '')     as context_campaign_medium
    , nullif(trim(context_page_referrer), '')       as context_page_referrer
    , nullif(trim(context_user_agent), '')          as context_user_agent
    , md5(nullif(trim(context_user_agent), ''))     as context_user_agent_id
    , nullif(trim(context_page_path), '')           as context_page_path
    , case
      when lower(name) = 'mytv' then 'home'
      when lower(name) = 'top playable' then 'top'
      else lower(name)
    end                                             as screen_name
    , nullif(trim(context_campaign_content), '')    as context_campaign_content
    , nullif(trim(context_campaign_content_id), '') as context_campaign_content_id
    , nullif(trim(url), '')                         as url

  from source
  where anonymous_id is not null

)

select * from renamed
{%- if target.name != 'prod' %}
  where received_at >= {{ dbt.dateadd('day', -incremental_dev_mode_days(), 'current_date') }}
{%- endif -%}
