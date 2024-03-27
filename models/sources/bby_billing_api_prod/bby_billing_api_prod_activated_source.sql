with

activated as (

  select * from {{ source('bby_billing_api_prod', 'activated') }}

)

, renamed as (

  select
    anonymous_id           as anonymous_id
    , activated_at         as activated_at

    , bby_contract_number  as bby_contract_number
    , bby_serial_number    as bby_serial_number
    , bby_sku              as bby_sku
    , bby_store_number     as bby_store_number
    , bby_transaction_date as bby_transaction_date
    , bby_price_type       as bby_price_type

    , received_at          as received_at
    , "timestamp"          as event_timestamp
  from activated

)

select * from renamed
