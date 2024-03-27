with

coupons as (

  select * from {{ ref('rails_prod_apply_coupon_succeeded_stage') }}

)

, coupon_accounts as (

  select
    coupons.account_id
    , 'all-coupon-' || lower(coupons.coupon_code) || '-redeemers' as audience
    , 'All Coupon ' || lower(coupons.coupon_code) || ' Redeemers' as audience_name
  from coupons

)

select * from coupon_accounts