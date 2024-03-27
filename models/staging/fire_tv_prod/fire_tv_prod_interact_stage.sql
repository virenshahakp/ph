{{
  config(
    materialized='incremental'
    , unique_key='id'
    , dist='even'
    , sort=['received_at']
  )
}}

{%- set max_received_at = incremental_max_value('received_at') %}

with

interact as (

  select * from {{ ref('fire_tv_prod_interact_source') }}

)

select * from interact

{%- if is_incremental() %}
  where received_at > {{ max_received_at }}
{%- endif %}
