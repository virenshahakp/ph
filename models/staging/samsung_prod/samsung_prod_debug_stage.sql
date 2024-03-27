{{
  config(
    materialized='incremental'
    , sort='loaded_at'
    , dist='playback_session_id' 
  )
}}

{%- set max_loaded_at = incremental_max_value('loaded_at') %}

with

debug as (

  select * from {{ ref('samsung_prod_debug_source') }}

)

select
  *
  , sysdate as dbt_processed_at
from debug
{% if is_incremental() %}
  where loaded_at > {{ max_loaded_at }}
{% endif %}
