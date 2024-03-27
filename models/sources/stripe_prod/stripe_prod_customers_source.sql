with

customers as (

  select
    id        as customer_id
    , philo_user_id
    , batch_timestamp
    , created as created_at
    , deleted
    , delinquent
    , state
    , tax_rate_id
    , zip
  from {{ source('stripe_prod', 'customers') }}

)

select * from customers