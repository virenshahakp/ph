with

coupon_applied as (

  select * from {{ source('rails_prod', 'apply_coupon_succeeded') }}

)

select
  user_id                                     as account_id
  , bearer_coupon                             as is_bearer_coupon
  , context_active                            as is_context_active
  , coupon_actions                            as coupon_actions
  , coupon_audiences                          as coupon_audiences
  , coupon_code                               as coupon_code
  , coupon_name                               as coupon_name
  , promotion                                 as promotion
  , received_at                               as received_at
  , subscriber_state                          as subscriber_state
  , event                                     as event
  , "timestamp"                               as applied_at
  , coalesce(subscriber_billing, 'chargebee') as subscriber_billing
from coupon_applied
