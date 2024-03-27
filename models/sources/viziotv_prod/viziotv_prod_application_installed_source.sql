with

installed as (

  select * from {{ source('viziotv_prod', 'application_installed') }}

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
    , context_device_advertising_id as context_device_advertising_id
    , context_page_url              as url
    , "timestamp"                   as visited_at
    , 'Vizio App Store'             as visit_type
    , 1                             as priority
    , null                          as coupon_code
    , null                          as reference
    , environment_analytics_version as environment_analytics_version
    , environment_version           as environment_app_version
    , received_at                   as received_at
    , uuid_ts                       as loaded_at
  from installed

)

select * from renamed
{%- if target.name != 'prod' %}
  where received_at >= {{ dbt.dateadd('day', -incremental_dev_mode_days(), 'current_date') }}
{%- endif -%}