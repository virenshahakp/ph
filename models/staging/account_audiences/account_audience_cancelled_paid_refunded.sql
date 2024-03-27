with

/*
  this logic is only valid for confirmed payment billers
  for an inferred payment biller (e.g. bestbuy) we don't
  get the refunded event we are simply told of a cancellation
*/

account_payments as (

  select * from {{ ref('rails_prod_payment_succeeded_stage') }}

)

, payment_refunded as (

  select * from {{ ref('rails_prod_payment_refunded_stage') }}

)

, cancellation_complete as (

  select * from {{ ref('rails_prod_cancellation_complete_stage') }}

)

, paid_subscribers as (

  select * from {{ ref('fct_paid_user_subscription_range') }}

)

, cancelled_accounts as (

  -- Get the most recent cancellation time and state of users, removing any currently active users
  select
    cancellation_complete.account_id
    , last_value(cancellation_complete.received_at)
      over (
        partition by cancellation_complete.account_id
        order by cancellation_complete.received_at
        rows between unbounded preceding and unbounded following
      )
    as last_cancelled_at
  from cancellation_complete
  join paid_subscribers on (cancellation_complete.account_id = paid_subscribers.account_id)
  where
    cancellation_complete.account_id not in (
      select account_id
      from paid_subscribers
      where is_active is true
    )

)

, payments as (

  -- accounts having only one successful payment
  select
    account_id
    , first_paid_at
  from account_payments
  {{ dbt_utils.group_by(n=2) }}
  having count(1) = 1

)

select
  payments.account_id
  , 'paid-1-mo-cancelled-with-refund'    as audience
  , 'Paid 1 Month, Cancelled (Refunded)' as audience_name
from payments
join cancelled_accounts on (payments.account_id = cancelled_accounts.account_id)
join payment_refunded on (payments.account_id = payment_refunded.account_id)