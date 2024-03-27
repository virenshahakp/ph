{{
  config(
    materialized='incremental'
    , sort='loaded_at'
    , dist='user_id' 
  )
}}

{%- set max_loaded_at = incremental_max_value('loaded_at') %}

with 

identifies as (

  select * from {{ ref('samsung_prod_identifies_source') }}

)

select 
  * 
  , sysdate as dbt_processed_at
from identifies
{% if is_incremental() %}
  where loaded_at > {{ max_loaded_at }}
{% endif %}
