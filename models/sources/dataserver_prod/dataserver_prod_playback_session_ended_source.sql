with

session_ended as (

  select * from {{ source('dataserver_prod', 'playback_session_ended') }}

)

, renamed as (

  select
    user_id       as user_id
    , received_at as received_at
    , uuid_ts     as loaded_at
    , pid         as playback_session_id
    , "timestamp" as session_ended_at

    , environment_analytics_version

  from session_ended

)

select * from renamed
{%- if target.name != 'prod' %}
  where received_at >= {{ dbt.dateadd('day', -incremental_dev_mode_days(), 'current_date') }}
{%- endif -%}
