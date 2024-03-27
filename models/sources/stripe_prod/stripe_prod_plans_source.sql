with

stripe_plans as (

  select
    id         as plan_id
    , amount   as plan_amount_cents
    , created  as created_at
    , batch_timestamp
    , currency
    , interval as billing_interval
    , interval_count
    , merchant_id
    , nickname
    , product_id
    , trial_period_days
  from {{ source('stripe_prod', 'plans') }}

)

select * from stripe_plans