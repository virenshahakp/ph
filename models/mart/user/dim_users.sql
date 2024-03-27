{{
  config(
    materialized='incremental'
    , unique_key='user_id'
    , sort=['account_id', 'user_id']
    , dist='user_id'
    , on_schema_change='append_new_columns'
  )
}}

{%- set max_loaded_at = incremental_max_value('loaded_at') %}

with

users as (

  select
    user_id
    , account_id
    , created_at
    , is_account_owner
    , has_email
    , has_phone
    , roles
    , subscriber_billing
    , subscriber_state
    , labels
    , packages
    , zip
    , dma_code
    , dma_region
    , dma_name
    , is_direct_billed
    , signup_source
    , loaded_at
  from {{ ref('rails_prod_users_stage') }}
  {% if is_incremental() %}
    where loaded_at > {{ max_loaded_at }}
  {% endif %}

)

select * from users
