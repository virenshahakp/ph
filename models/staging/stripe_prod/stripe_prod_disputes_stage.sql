{{
  config(
    materialized='incremental'
    , dist='charge_id'
    , sort='batch_timestamp'
    , unique_key='dispute_id'
  )
}}

{%- set max_loaded_at = incremental_max_value('batch_timestamp') %}

with

disputes as (

  select * from {{ ref('stripe_prod_disputes_source') }}
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
  disputes.*
  , invoices.invoice_id
from disputes
join invoices on disputes.charge_id = invoices.charge_id