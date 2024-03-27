with

modified as (

  select * from {{ source('bby_billing_api_prod', 'modified') }}

)

, renamed as (

  select
    anonymous_id           as anonymous_id
    , modified_at          as modified_at
    , modify_reason_code   as modify_reason_code
    , bby_contract_number  as bby_contract_number

    , bby_serial_number    as bby_serial_number
    , bby_sku              as bby_sku
    , bby_store_number     as bby_store_number
    , bby_transaction_date as bby_transaction_date
    , received_at          as received_at

    , "timestamp"          as event_timestamp
    , case
      when modify_reason_code = '01'
        then 'credit card expiring'
      when modify_reason_code = '02'
        then 'credit card update'
      when modify_reason_code = '03'
        then 'set auto renew to yes'
      when modify_reason_code = '04'
        then 'set auto renew to no'
      when modify_reason_code = '05'
        then 'add promo pricing to a subscription'
      else 'unknown'
    end                    as modify_reason_text
  from modified

)

select * from renamed
