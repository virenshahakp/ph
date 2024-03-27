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

  select * from {{ ref('fire_tv_prod_stream_end_source') }}

)

select 
  *
  , sysdate as dbt_processed_at
from stream_end
{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where loaded_at > {{ max_loaded_at }}

{% endif %}