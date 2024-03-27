with

backgrounded as (

  select * from {{ source('viziotv_prod', 'application_backgrounded') }}

)

, renamed as (

  select -- noqa: L034
    id                              as event_id
    , lower(anonymous_id)           as anonymous_id
    , lower(user_id)                as user_id
    , hashed_session_id             as hashed_session_id
    , nullif(context_ip, '')        as context_ip
    , context_user_agent            as context_user_agent
    , context_page_path             as context_page_path
    , context_page_title            as context_page_title
    , "timestamp"                   as "timestamp"
    , null                          as reference
    , environment_analytics_version as environment_analytics_version
    , environment_version           as environment_app_version
    , received_at                   as received_at
    , uuid_ts                       as loaded_at
  from backgrounded

)

select * from renamed
{%- if target.name != 'prod' %}
  where received_at >= {{ dbt.dateadd('day', -incremental_dev_mode_days(), 'current_date') }}
{%- endif -%}