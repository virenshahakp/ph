with

redemptions as (

  select * from {{ source('airbyte', 'bestbuy_coupons_redeemed') }}

)

, renamed as (

  select
    offer                        as offer
    , bby_sku                    as bby_sku
    , bby_serial_number          as bby_serial_number
    , bby_transaction_date::date as bby_transaction_date
  from redemptions

)

select * from renamed
