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

rebuffering_end as (

  select * from {{ ref('web_prod_rebuffering_end_source') }}

)

select
  *
  , sysdate as dbt_processed_at
from rebuffering_end
{% if is_incremental() %}
  where loaded_at > {{ max_loaded_at }}
{% endif %}