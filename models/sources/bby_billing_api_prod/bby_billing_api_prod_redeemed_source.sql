with

redeemed as (

  select * from {{ source('bby_billing_api_prod', 'redeemed') }}

)

, renamed as (

  select
    anonymous_id           as anonymous_id
    , redemption_date      as redeemed_at

    , bby_contract_number  as bby_contract_number
    , bby_serial_number    as bby_serial_number
    , bby_sku              as bby_sku
    , bby_store_number     as bby_store_number
    , bby_transaction_date as bby_transaction_date

    , received_at          as received_at
    , "timestamp"          as event_timestamp
  from redeemed

)

select * from renamed
