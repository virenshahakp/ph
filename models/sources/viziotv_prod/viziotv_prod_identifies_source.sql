with

identifies as (

  select * from {{ source('viziotv_prod', 'identifies') }}

)

, renamed as (

  select
    environment_analytics_version as environment_analytics_version
    , environment_version         as environment_app_version
    , received_at                 as received_at
    , "timestamp"                 as "timestamp"
    , uuid_ts                     as loaded_at
    , lower(anonymous_id)         as anonymous_id
    , lower(user_id)              as user_id
    , nullif(context_ip, '')      as context_ip
  from identifies

)

select * from renamed
{%- if target.name != 'prod' %}
  where received_at >= {{ dbt.dateadd('day', -incremental_dev_mode_days(), 'current_date') }}
{%- endif -%}