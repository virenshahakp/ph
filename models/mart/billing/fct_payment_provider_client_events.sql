{{
  config(
    materialized='incremental'
    , unique_key='id'
    , dist='user_id'
    , sort='received_at'
  )
}}

with

events as (

  select *
  from
    {{ dbt_utils.union_relations(
        relations=[
          ref('all_platforms_payment_provider_purchase_error')
          , ref('all_platforms_payment_provider_purchase_success')
          , ref('all_platforms_payment_provider_purchase_start')
        ], include=common_columns() + [
          "sku"
          , "code"
          , "provider_name"
          , "event"
          , "platform"
        ]
      )
    }}
)

select
  events.*
  , {{ dbt_utils.generate_surrogate_key(['event', 'id']) }} as event_id
from events
{%- if is_incremental() %}
  where received_at >= {{ dbt.dateadd('day', -incremental_recent_days(), 'current_date') }}
{%- endif %}