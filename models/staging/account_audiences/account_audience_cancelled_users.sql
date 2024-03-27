with

paid_subscribers as (

  select * from {{ ref('fct_paid_user_subscription_range') }}

)

, cancellation_complete as (

  select * from {{ ref('rails_prod_cancellation_complete_stage') }}

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
    , last_value(cancellation_complete.subscriber_state)
      over (
        partition by cancellation_complete.account_id
        order by cancellation_complete.received_at
        rows between unbounded preceding and unbounded following
      )
    as last_cancelled_state
  from cancellation_complete
  join paid_subscribers on (cancellation_complete.account_id = paid_subscribers.account_id)
  where
    cancellation_complete.account_id not in (
      select account_id
      from paid_subscribers
      where is_active is true
    )

)

select distinct
  account_id
  , 'cancelled' as audience
  , 'Cancelled' as audience_name
  -- , last_cancelled_at
  -- , last_cancelled_state
from cancelled_accounts