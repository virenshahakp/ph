with

source as (

  select * from {{ source('roku_prod', 'launch') }}

)

, renamed as (

  select
    id                                as event_id
    , anonymous_id                    as anonymous_id
    , user_id                         as user_id
    , uuid_ts                         as loaded_at
    , source                          as source
    , 'roku'                          as url
    , "timestamp"                     as visited_at
    -- swap out two lines below after the release
    --, coalesce(fast_account_request, false) as has_fast_account_request
    , false                           as has_fast_account_request
    , null                            as coupon_code
    , null                            as reference
    , {{ normalize_id("content_id") }}                                   as context_campaign_content_id
    , ''                              as context_page_referrer
    , 'roku'                          as context_user_agent
    , 'roku'                          as context_page_path
    , nullif(context_ip, '')          as context_ip
    , case
      when source like 'ad%'
        then
          case
          -- AP: 2019/02/01 Roku changed their utm source from string to array, we have to fix it here
            when utm_source = '["roku"]' then 'roku'
            when utm_source = 'roku' then 'roku'
            when utm_source is null then 'roku-non-display'
            else utm_source
          end
      else utm_source
    end                               as context_campaign_source
    , coalesce(utm_campaign, 'philo') as context_campaign_name
    , case
      when source like 'ad%' and utm_source is null then 'philo-roku-unknown'
      else coalesce(utm_term, 'philo')
    end                               as context_campaign_term
    , case
      when source like 'ad%' and utm_source is null then 'philo-roku-unknown'
      else coalesce(utm_medium, 'philo')
    end                               as context_campaign_medium
    , case
      when source like 'ad%' and utm_source is null then 'roku'
      else utm_content
    end                               as context_campaign_content
    , context_device_advertising_id
  from source
  where anonymous_id is not null

)

select * from renamed
{%- if target.name != 'prod' %}
  where loaded_at >= {{ dbt.dateadd('day', -incremental_dev_mode_days(), 'current_date') }}
{%- endif -%}
