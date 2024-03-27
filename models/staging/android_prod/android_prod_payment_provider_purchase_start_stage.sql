with

payment_provider_purchase_start as (

  select * from {{ ref('android_prod_payment_provider_purchase_start_source') }}

)

select * from payment_provider_purchase_start
