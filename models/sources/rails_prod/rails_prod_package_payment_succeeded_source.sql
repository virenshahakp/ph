with

successful_payments as (

  select * from {{ source('rails_prod', 'package_payment_succeeded') }}

)

, renamed as (

  select
    id                 as package_payment_succeeded_id
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
    , event            as event -- noqa: disable=L029
    , received_at      as received_at
    , uuid_ts          as loaded_at
  from successful_payments

)

select * from renamed
