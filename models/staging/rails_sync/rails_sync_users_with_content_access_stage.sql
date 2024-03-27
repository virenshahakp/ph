{{ 
  config(
    materialized='incremental'
    , sort=['reported_at']
    , dist='account_id'
  )
}}

{%- set max_loaded_at = incremental_max_value('reported_at') %}
{% set first_sync_at = '2022-01-12 15:23:00' %}

with

rails_accounts as (

  select *
  from {{ ref("rails_sync_users_with_content_access_source") }}
  where
    -- the first day the sync was able to be used for content access evaluations
    reported_at >= '{{ first_sync_at }}'
  {%- if is_incremental() %}
      and reported_at > {{ max_loaded_at }}
    {% endif %}

)

select
  rails_accounts.*
  , {{ dbt_utils.generate_surrogate_key(['subscriber_billing', 'activated_plan', 'product_sku', 'status']) }} as subscription_hash
from rails_accounts