with

updated as (

  select * from {{ source('samsung_prod', 'application_updated') }}

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
    , url                           as url
    , "timestamp"                   as "timestamp"
    , environment_analytics_version as environment_analytics_version
    , environment_version           as environment_app_version
    , received_at                   as received_at
    , uuid_ts                       as loaded_at
  from updated

)

select * from renamed
{%- if target.name != 'prod' %}
  where received_at >= {{ dbt.dateadd('day', -incremental_dev_mode_days(), 'current_date') }}
{%- endif -%}
