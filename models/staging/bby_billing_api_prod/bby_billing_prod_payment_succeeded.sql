/*
  we take some purchase information from the "activated" event
  which occurs at time of purchase and the rest from the "redeemed"
  event which occurs when the subscriber first creates the Philo
  account to link the best buy purchase to the Philo account
*/

with

activated as (

  select * from {{ ref('bby_billing_api_prod_activated_stage') }}

)

, redeemed as (

  select * from {{ ref('bby_billing_api_prod_redeemed_stage') }}

)

, bestbuy_identifies as (

  select * from {{ ref('bby_billing_api_prod_identifies_stage') }}

)

, payment_succeeded as (

  select
    bestbuy_identifies.account_id as account_id
    , redeemed.received_at        as received_at -- when philo account was created and activation code redeemed
    , activated.activated_at      as activated_at -- when payments started and renewal dates will be calculated
    , activated.packages          as packages
    -- casting to avoid an unknown type error in testing against accepted_values that are longer than varchar(7)
    , 'regular'::varchar(64)      as subscriber_state
    , 'bestbuy'                   as subscriber_billing
    , activated.list_price        as list_price
    , false                       as is_gift  -- this price used for future payment amounts on the recurring subscription
    , true                        as is_active
    , activated.bby_price_type    as bby_price_type
    , 'payment_succeeded'         as event
    , case
      when activated.bby_price_type = 0
        then activated.list_price
      else 0
    end                           as amount
  from activated
  join redeemed on (activated.bby_serial_number = redeemed.bby_serial_number)
  join bestbuy_identifies on (redeemed.anonymous_id = bestbuy_identifies.anonymous_id)

)

select * from payment_succeeded
