{{
  config(
    materialized='incremental'
    , dist='product_id'
    , sort='batch_timestamp'
    , unique_key='product_id'
  )
}}

with

{%- set max_loaded_at = incremental_max_value('batch_timestamp') %}

products as (

  select * from {{ ref('stripe_prod_products_source') }}
  {% if is_incremental() %}
    where batch_timestamp > {{ max_loaded_at }}
  {% endif %}

)

select * from products