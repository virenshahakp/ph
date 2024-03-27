{{
  config(
    materialized='incremental'
    , dist='invoice_id'
    , sort='batch_timestamp'
    , unique_key='invoice_id'
  )
}}

with

{%- set max_loaded_at = incremental_max_value('batch_timestamp') %}

invoices as (

  select * from {{ ref('stripe_prod_invoices_source') }}
  {% if is_incremental() %}
    where batch_timestamp > {{ max_loaded_at }}
  {% endif %}

)

select * from invoices