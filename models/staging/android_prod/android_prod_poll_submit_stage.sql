{{
  config(
    materialized='incremental'
    , dist='user_id'
    , sort=['loaded_at', 'received_at']
  )
}}

{%- set max_loaded_at = incremental_max_value('loaded_at') %}

with

poll_submit as (

  select * from {{ ref('android_prod_poll_submit_source') }}

)

select
  *
  , sysdate as dbt_processed_at
from poll_submit
{%- if is_incremental() %}
  where loaded_at > {{ max_loaded_at }}
{%- endif %}