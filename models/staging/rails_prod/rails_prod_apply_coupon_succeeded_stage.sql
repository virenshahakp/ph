with 

coupons as (

  select * from {{ ref('rails_prod_apply_coupon_succeeded_source') }}

)

select * from coupons
