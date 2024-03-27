with

cancelled as (

  -- renaming canceled to cancelled for consistency with other events in our system
  select * from {{ source('bby_billing_api_prod', 'canceled') }}

)

, renamed as (

  select
    anonymous_id           as anonymous_id
    , cancelled_at         as cancelled_at
    , cancel_reason_code   as cancel_reason_code
    , end_date             as end_date
    , bby_contract_number  as bby_contract_number

    , bby_serial_number    as bby_serial_number
    , bby_sku              as bby_sku
    , bby_store_number     as bby_store_number
    , bby_transaction_date as bby_transaction_date
    , received_at          as received_at

    , "timestamp"          as event_timestamp
    , case
      when cancel_reason_code = '100'
        then 'immediate cancel'
      when cancel_reason_code = '200'
        then 'pending cancel (i.e. at end of billing cycle)'
      when cancel_reason_code = '210'
        then 'failed auth'
    end                    as cancel_reason_text
  from cancelled

)

select * from renamed
