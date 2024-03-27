with

paid_subscribers as (

  select * from {{ ref('fct_paid_user_subscription_range') }}

)

, coupons as (

  select * from {{ ref('rails_prod_apply_coupon_succeeded_stage') }}

)

, active_coupon_accounts as (

  select
    paid_subscribers.account_id
    , 'all-active-coupon-' || lower(coupons.coupon_code) || '-payers' as audience
    , 'All Active Coupon ' || lower(coupons.coupon_code) || ' Payers' as audience_name
  from paid_subscribers
  join coupons on (paid_subscribers.account_id = coupons.account_id)
  where
    paid_subscribers.is_active is true

)

select * from active_coupon_accounts