{{
  config(
    materialized='incremental'
    , dist='invoice_id'
    , sort='batch_timestamp'
    , unique_key='invoice_line_item_id'
  )
}}

with

{%- set max_loaded_at = incremental_max_value('batch_timestamp') %}

invoice_items as (

  select * from {{ ref('stripe_prod_invoice_line_items_source') }}
  {% if is_incremental() %}
    where batch_timestamp > {{ max_loaded_at }}
  {% endif %}

)

select * from invoice_items