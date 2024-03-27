with

activated as (

  select * from {{ ref('bby_billing_api_prod_activated_source') }}

)

, bestbuy as (

  select * from {{ ref('bestbuy_sku_price_package') }}

)

select
  activated.*
  , bestbuy.list_price
  , bestbuy.packages
from activated
left join bestbuy on (activated.bby_sku = bestbuy.bby_sku)
where activated.bby_store_number != 'test'
  -- date of bestbuy launch to remove test data
  and activated.activated_at >= '2020-09-24'

