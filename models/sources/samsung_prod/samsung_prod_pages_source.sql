with

pages as (

  select * from {{ source('samsung_prod', 'pages') }}

)

, renamed as (

  select
    id                                                             as event_id
    , environment_analytics_version                                as environment_analytics_version
    , environment_version                                          as app_version
    , "timestamp"                                                  as visited_at
    , received_at                                                  as received_at
    , uuid_ts                                                      as loaded_at
    , lower(anonymous_id)                                          as anonymous_id
    , lower(user_id)                                               as user_id
    , nullif(trim(context_ip), '')                                 as context_ip
    , nullif(trim(context_page_referrer), '')                      as context_page_referrer
    , nullif(trim(context_user_agent), '')                         as context_user_agent
    , md5(nullif(trim(context_user_agent), ''))                    as context_user_agent_id
    , nullif(trim(context_page_path), '')                          as context_page_path
    , coalesce(lower(name), split_part(context_page_path, '/', 3)) as screen_name
    , nullif(trim(context_page_url), '')                           as context_page_url
  from pages

)

select * from renamed
{%- if target.name != 'prod' %}
  where received_at >= {{ dbt.dateadd('day', -incremental_dev_mode_days(), 'current_date') }}
{%- endif -%}