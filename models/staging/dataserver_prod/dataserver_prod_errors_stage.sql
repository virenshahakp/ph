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

errors as (

  select
    user_id
    , playback_session_id
    , requested_asset_id
    , platform
    , player_id
    , received_at
    , loaded_at
    , event_timestamp
    , event_text
    , error_type
    , error_code
    , error_description
    , environment_analytics_version
    , md5(nullif(trim(context_user_agent), '')) as context_user_agent_id
    , sysdate                                   as dbt_processed_at
  from {{ ref('dataserver_prod_errors_source') }}

)

, agents as (

  select
    context_user_agent_id
    , os_family
    , os_version_major
    , nullif(os_version_minor, '') as os_version_minor
    , nullif(os_version_patch, '') as os_version_patch
  from {{ ref('dim_user_agents') }}

)

select
  errors.*
  , agents.os_family
  , agents.os_version_major
  , agents.os_version_minor
  , agents.os_version_patch
  , agents.os_version_major
  || '.' || coalesce(agents.os_version_minor, '0')
  || '.' || coalesce(agents.os_version_patch, '0')
  as os_version
from errors
left join agents
  on (errors.context_user_agent_id = agents.context_user_agent_id)
{% if is_incremental() %}
  where errors.loaded_at > {{ max_loaded_at }}
{% endif %}
