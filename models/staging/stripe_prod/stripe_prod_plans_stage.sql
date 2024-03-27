{{
  config(
    materialized='incremental'
    , dist='plan_id'
    , sort='batch_timestamp'
    , unique_key='plan_id'
  )
}}

with

{%- set max_loaded_at = incremental_max_value('batch_timestamp') %}

plans as (

  select * from {{ ref('stripe_prod_plans_source') }}
  {% if is_incremental() %}
    where batch_timestamp > {{ max_loaded_at }}
  {% endif %}

)

select * from plans