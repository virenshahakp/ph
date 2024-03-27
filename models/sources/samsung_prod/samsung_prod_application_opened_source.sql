with

opened as (

  select * from {{ source('samsung_prod', 'application_opened') }}

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
    , context_page_url              as url

    -- set constants for visit attribution fields
    , 'samsung'                     as context_campaign_source
    , 'philo'                       as context_campaign_name
    , 'philo'                       as context_campaign_term
    , 'philo'                       as context_campaign_medium
    , 'samsung'                     as context_campaign_content
    , null                          as context_campaign_content_id
    , "timestamp"                   as visited_at
    , 2                             as priority
    , 'samsung'                     as visit_type
    , null                          as coupon_code
    , null                          as reference

    , environment_analytics_version as environment_analytics_version
    , environment_version           as environment_app_version
    , received_at                   as received_at
    , uuid_ts                       as loaded_at
  from opened

)

select * from renamed
{%- if target.name != 'prod' %}
  where received_at >= {{ dbt.dateadd('day', -incremental_dev_mode_days(), 'current_date') }}
{%- endif -%}
