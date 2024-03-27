{{
  config(
    materialized='incremental'
    , dist='charge_id'
    , sort='batch_timestamp'
    , unique_key='refund_id'
  )
}}

{%- set max_loaded_at = incremental_max_value('batch_timestamp') %}

with

refunds as (

  select * from {{ ref('stripe_prod_refunds_source') }}
  {% if is_incremental() %}
    where batch_timestamp > {{ max_loaded_at }}
  {% endif %}

)

, invoices as (

  select
    charge_id
    , invoice_id
  from {{ ref('stripe_prod_invoices_stage') }}

)

select
  refunds.*
  , invoices.invoice_id
from refunds
join invoices on refunds.charge_id = invoices.charge_id