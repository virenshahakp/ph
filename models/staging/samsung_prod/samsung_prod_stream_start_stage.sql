{{
  config(
    materialized='incremental'
    , dist='playback_session_id'
    , sort='loaded_at'
    , on_schema_change = 'append_new_columns'
  )
}}

{%- set max_loaded_at = incremental_max_value('loaded_at') %}

with

stream_start as (

  select * from {{ ref('samsung_prod_stream_start_source') }}

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
  stream_start.* 
  , agents.os_family
  , agents.os_version_major
  , agents.os_version_minor
  , agents.os_version_patch
  , agents.os_version_major
  || '.' || coalesce(agents.os_version_minor, '0') 
  || '.' || coalesce(agents.os_version_patch, '0') 
  as os_version
  , sysdate as dbt_processed_at
from stream_start
left join agents on (stream_start.context_user_agent_id = agents.context_user_agent_id)
{% if is_incremental() %}
  where stream_start.loaded_at > {{ max_loaded_at }}
{% endif %}
