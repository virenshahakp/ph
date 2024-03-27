{{
  config(
    materialized='incremental'
    , dist='playback_session_id'
    , sort='dbt_processed_at'
    , on_schema_change = 'append_new_columns'
  )
}}

{%- set max_loaded_at = incremental_max_value('loaded_at') %}

with

stream_error as (

  select
    {{ qoe_columns_select(skip_columns=[
        'device_name'
        , 'device_manufacturer'
        , 'device_model'
        , 'os_version'
        , 'position'
        , 'position_ms'
      ])
    }}
    , context_user_agent_id
    , error_description
    , error_philo_code
    , error_detailed_name
    , error_http_status_code
    , {{ try_cast_numeric('raw_error_code', 'bigint') }}           as error_code
    , sysdate             as dbt_processed_at
  from {{ ref('samsung_prod_stream_error_source') }}

)

, agents as (

  select
    context_user_agent_id
    , os_family
    , os_version_major
    , nullif(os_version_minor, '') as os_version_minor
    , nullif(os_version_patch, '') as os_version_patch
  from {{ ref('dim_user_agents') }}
  where os_family = 'Tizen'

)

select
  stream_error.*
  , agents.os_family
  , agents.os_version_major
  , agents.os_version_minor
  , agents.os_version_patch
  , agents.os_version_major
  || '.' || coalesce(agents.os_version_minor, '0')
  || '.' || coalesce(agents.os_version_patch, '0')
  as os_version
from stream_error
left join agents on (stream_error.context_user_agent_id = agents.context_user_agent_id)
{% if is_incremental() %}
  where stream_error.loaded_at > {{ max_loaded_at }}
{% endif %}
