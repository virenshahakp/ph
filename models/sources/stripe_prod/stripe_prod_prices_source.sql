with

prices as (

  select
    id            as price_id
    , unit_amount as price_cents
    , created     as price_created_at
    , batch_timestamp
    , merchant_id
    , product_id
    , nickname
    , recurring_interval
    , recurring_interval_count
    , recurring_trial_period_days
  from {{ source('stripe_prod', 'prices') }}

)

select * from prices