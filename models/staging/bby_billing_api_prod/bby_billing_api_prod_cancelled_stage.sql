with

cancellations as (

  select * from {{ ref('bby_billing_api_prod_cancelled_source') }}

)

, bestbuy as (

  select * from {{ ref('bestbuy_sku_price_package') }}

)

, cancelled as (

  select
    cancellations.*
    , case
      when cancellations.cancel_reason_code = 100 then cancellations.received_at -- immediate cancellation
      when cancellations.cancel_reason_code = 200 then cancellations.end_date    -- cancellation at end of billing period
      when cancellations.cancel_reason_code = 210 then {{ dbt.dateadd('day', 7, 'cancellations.received_at') }} -- delinquent_access, no access in 7 days
    end as cancellation_effective_at
    , bestbuy.packages
  from cancellations
  left join bestbuy on (cancellations.bby_sku = bestbuy.bby_sku)

)

select * from cancelled
