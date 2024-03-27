{{
  config(
    materialized='incremental'
    , dist='playback_session_id'
    , sort='dbt_processed_at'
    , on_schema_change='append_new_columns'
  )
}}

{%- set max_loaded_at = incremental_max_value('loaded_at') %}

with

stream_start as (

  select * from {{ ref('chromecast_prod_stream_start_source') }}

)

select
  *
  , sysdate as dbt_processed_at
from stream_start
{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where loaded_at > {{ max_loaded_at }}

{% endif %}