with

redeemed as (

  select * from {{ ref('bby_billing_api_prod_redeemed_source') }}

)

select * from redeemed
