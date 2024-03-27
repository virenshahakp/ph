{{
  config(
    materialized='incremental'
    , unique_key='playback_session_id'
    , dist='playback_session_id'
    , sort='received_at'
  )
}}

{%- set max_dbt_processed_at = incremental_max_value('dbt_processed_at') %}

with attributes as (

  select
    playback_session_id
    , user_id
    , hashed_session_id
    , requested_asset_id as asset_id -- legacy column support
    , requested_asset_id
    , played_asset_id
    , is_wifi
    , is_cellular
    , app_version
    , os_version
    , screen_height
    , screen_width
    , client_ip
    , device_name
    , device_manufacturer
    , device_model
    , platform
    , received_at
    , dbt_processed_at
  from {{ ref('fct_stream_events') }}
  where event_index = 1
  {%- if is_incremental() %}
    and dbt_processed_at > {{ max_dbt_processed_at }}
  {%- endif %}

)

select * from attributes