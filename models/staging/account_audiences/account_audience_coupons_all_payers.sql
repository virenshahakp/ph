with

paid_subscribers as (

  select * from {{ ref('fct_paid_user_subscription_range') }}

)

, coupons as (

  select * from {{ ref('rails_prod_apply_coupon_succeeded_stage') }}

)

, coupon_accounts as (

  select
    paid_subscribers.account_id
    , 'all-coupon-' || lower(coupons.coupon_code) || '-payers' as audience
    , 'All Coupon ' || lower(coupons.coupon_code) || ' Payers' as audience_name
  from paid_subscribers
  join coupons on (paid_subscribers.account_id = coupons.account_id)

)

select * from coupon_accounts