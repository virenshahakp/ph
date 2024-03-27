{{
  config(
    materialized='incremental'
    , dist='philo_user_id'
    , sort='batch_timestamp'
    , unique_key='customer_id'
  )
}}

with

{%- set max_loaded_at = incremental_max_value('batch_timestamp') %}

customers as (

  select * from {{ ref('stripe_prod_customers_source') }}
  {% if is_incremental() %}
    where batch_timestamp > {{ max_loaded_at }}
  {% endif %}

)

select * from customers