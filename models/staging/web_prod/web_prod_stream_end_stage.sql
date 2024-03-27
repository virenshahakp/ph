{{
  config(
    materialized='incremental'
    , dist='playback_session_id'
    , sort='dbt_processed_at'
  )
}}

{%- set max_loaded_at = incremental_max_value('loaded_at') %}

with

stream_end as (

  select * from {{ ref('web_prod_stream_end_source') }}

)

select
  *
  , sysdate as dbt_processed_at
from stream_end
{% if is_incremental() %}
  where loaded_at > {{ max_loaded_at }}
{% endif %}