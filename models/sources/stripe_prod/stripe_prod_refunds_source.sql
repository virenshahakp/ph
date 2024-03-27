with

refunds as (

  select
    id        as refund_id
    , amount  as refund_amount_cents
    , created as refund_created_at
    , reason  as refund_reason
    , status  as refund_status
    , charge_id
    , batch_timestamp
    , failure_reason
    , currency
    , receipt_number
  from {{ source('stripe_prod', 'refunds') }}

)

select * from refunds