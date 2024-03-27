{{
  config(
    materialized='incremental'
    , unique_key='id'
    , dist='user_id'
    , sort='received_at'
  )
}}

with

all_platforms as (

  {{ dbt_utils.union_relations(
      relations=[
        ref('android_prod_payment_provider_purchase_start_stage')
        , ref('androidtv_prod_payment_provider_purchase_start_stage')
        , ref('fire_prod_payment_provider_purchase_start_stage')
        , ref('fire_tv_prod_payment_provider_purchase_start_stage')
        , ref('ios_prod_payment_provider_purchase_start_stage')
        , ref('roku_prod_payment_provider_purchase_start_stage')
        , ref('tvos_prod_payment_provider_purchase_start_stage')
      ]
      , include=common_columns() + [
        "sku"
        , "provider_name"
      ]
    )
  }}
)

, add_platform as (

  select
    {{ columns_select(common_columns()) }}
    , sku
    , 'payment_provider_purchase_start' as "event" -- noqa: L059
    , lower(provider_name)              as provider_name
    , {{ get_platform_from_union_relations(_dbt_source_relation) }}                                       as platform
  from all_platforms
)

select *
from add_platform
{%- if is_incremental() %}
  where received_at >= {{ dbt.dateadd('day', -incremental_recent_days(), 'current_date') }}
{%- endif %}