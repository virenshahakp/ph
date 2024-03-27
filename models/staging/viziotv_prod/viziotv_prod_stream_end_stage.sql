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

stream_end as (

  select * from {{ ref('viziotv_prod_stream_end_source') }}

)

select
  *
  , sysdate as dbt_processed_at
from stream_end
{% if is_incremental() %}
  where loaded_at > {{ max_loaded_at }}
{% endif %}