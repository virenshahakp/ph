with

source as (

  select * from {{ source('dataserver_prod', 'error') }}

)

, renamed as (

  select
    user_id                         as user_id
    , pid                           as playback_session_id
    , device_type                   as platform
    , player_id                     as player_id
    , received_at                   as received_at
    , uuid_ts                       as loaded_at
    , original_timestamp            as event_timestamp
    , event_text                    as event_text
    , operation                     as error_type
    , error_code                    as error_code
    , philo_code                    as error_description
    , user_agent                    as context_user_agent
    , environment_analytics_version
    as environment_analytics_version
    , {{ normalize_id("requested_asset_id") }}                                 as requested_asset_id
  from source
)

select * from renamed
{%- if target.name != 'prod' %}
  where loaded_at >= {{ 
    dbt.dateadd(
      'day'
      , -incremental_dev_mode_days()
      , 'current_date'
    ) 
  }}
{%- endif -%}