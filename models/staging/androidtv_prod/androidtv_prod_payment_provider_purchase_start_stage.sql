with

payment_provider_purchase_start as (

  select * from {{ ref('androidtv_prod_payment_provider_purchase_start_source') }}

)

select * from payment_provider_purchase_start
