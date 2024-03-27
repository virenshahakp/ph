with

disputes as (

  select
    id        as dispute_id
    , amount  as dispute_amount_cents
    , created as dispute_created_at
    , reason  as dispute_reason
    , status  as dispute_status
    , charge_id
    , batch_timestamp
    , network_reason_code
  from {{ source('stripe_prod', 'disputes') }}

)

select * from disputes