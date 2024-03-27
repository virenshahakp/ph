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

browse_while_watching_activated as (

  select * from {{ ref('roku_prod_browse_while_watching_activated_source') }}

)

select 
  *
  , sysdate as dbt_processed_at
from browse_while_watching_activated
{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where loaded_at > {{ max_loaded_at }}

{% endif %}