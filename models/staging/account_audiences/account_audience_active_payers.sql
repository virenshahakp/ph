with

paid_subscribers as (

  select * from {{ ref('fct_paid_user_subscription_range') }}

)

, active_accounts as (

  select
    paid_subscribers.account_id
    , 'all-active-payers' as audience
    , 'All Active Payers' as audience_name
  from paid_subscribers
  where
    paid_subscribers.is_active is true

)

select * from active_accounts