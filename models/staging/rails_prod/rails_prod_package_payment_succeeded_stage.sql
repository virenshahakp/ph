{{ config(
  materialized='incremental'
  , dist='account_id'
  , sort=['received_at']
  , tags=["daily", "exclude_hourly"]
  )
}}

{%- set max_loaded_at = incremental_max_value('loaded_at') %}

with

payment_succeeded as (

  select *

  from {{ ref('rails_prod_package_payment_succeeded_source') }}
  {% if is_incremental() %}
    where loaded_at > {{ max_loaded_at }}
  {% endif %}

)

select * from payment_succeeded