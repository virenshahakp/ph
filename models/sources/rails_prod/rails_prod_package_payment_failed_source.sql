with

failed_payments as (

  select * from {{ source('rails_prod', 'package_payment_failed') }}

)

, renamed as (

  select
    id                 as package_payment_failed_id
    , user_id          as account_id
    , package          as package
    , pakage_type      as package_type
    , packages         as packages
    , invoice          as invoice
    , total_bill_cents as total_bill_cents
    , proration_cents  as proration_cents
    , amount_cents     as amount_cents
    , proration_start  as proration_start
    , proration_end    as proration_end
    , access_start     as access_start
    , access_end       as access_end
    , event            as event
    , received_at      as received_at
  from failed_payments

)

select * from renamed
