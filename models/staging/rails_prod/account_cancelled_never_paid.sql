with

cancellation_complete as (

  select * from {{ ref('rails_prod_cancellation_complete_stage') }}

)

, payment_succeeded as (

  select * from {{ ref('rails_prod_payment_succeeded_stage') }}

)


select
  cancellation_complete.account_id
  , 'subscription_cancelled_never_paid' as event -- noqa: L029
  , cancellation_complete.subscriber_billing
  , cancellation_complete.received_at
from cancellation_complete
left join payment_succeeded on (cancellation_complete.account_id = payment_succeeded.account_id)
where cancellation_complete.received_at >= '2017-11-14'
  and payment_succeeded.account_id is null
