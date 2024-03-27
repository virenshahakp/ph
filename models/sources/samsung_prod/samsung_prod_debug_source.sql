with

debug as (

  select * from {{ source('samsung_prod', 'debug') }}

)

, renamed as (

  select
    id                                              as event_id
    , name                                          as event_name -- noqa:disable=RF04
    , environment_analytics_version                 as environment_analytics_version
    , environment_os_version                        as environment_os_version
    , environment_version                           as environment_app_version
    , original_timestamp                            as original_timestamp
    , "timestamp"                                   as event_timestamp
    , received_at                                   as received_at
    , uuid_ts                                       as loaded_at
    , gap_size                                      as gap_size
    , position                                      as position
    , context_device_performance_used_jsheap_size   as device_used_jsheap_size
    , context_device_tizen_cpuload                  as device_tizen_cpuload
    , context_device_performance_total_jsheap_size  as device_total_jsheap_size
    , context_device_performance_js_heap_size_limit as device_js_heap_size_limit
    , context_device_tizen_memory_status            as device_tizen_memory_status
    , lower(anonymous_id)                           as anonymous_id
    , lower(user_id)                                as user_id
    , lower(session_id)                             as playback_session_id
    , nullif(trim(context_ip), '')                  as context_ip
    , nullif(trim(context_page_referrer), '')       as context_page_referrer
    , nullif(trim(context_user_agent), '')          as context_user_agent
    , md5(nullif(trim(context_user_agent), ''))     as context_user_agent_id
    , nullif(trim(context_page_path), '')           as context_page_path
    , nullif(trim(context_page_url), '')            as context_page_url
  from debug

)

select * from renamed
{%- if target.name != 'prod' %}
  where received_at >= {{ dbt.dateadd('day', -incremental_dev_mode_days(), 'current_date') }}
{%- endif -%}