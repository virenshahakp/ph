{{
  config(
    materialized='incremental'
    , dist='price_id'
    , sort='batch_timestamp'
    , unique_key='price_id'
  )
}}

{%- set max_loaded_at = incremental_max_value('batch_timestamp') %}

with

prices as (

  select * from {{ ref('stripe_prod_prices_source') }}
  {% if is_incremental() %}
    where batch_timestamp > {{ max_loaded_at }}
  {% endif %}

)

select * from prices