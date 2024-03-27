{{
  config(
    materialized='incremental'
    , dist='invoice_id'
    , sort='batch_timestamp'
    , unique_key='charge_id'
  )
}}

{%- set max_loaded_at = incremental_max_value('batch_timestamp') %}

with

charges as (

  select * from {{ ref('stripe_prod_charges_source') }}
  {% if is_incremental() %}
    where batch_timestamp > {{ max_loaded_at }}
  {% endif %}

)

select * from charges