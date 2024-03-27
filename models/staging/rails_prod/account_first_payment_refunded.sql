with

users as (

  select dim_users.account_id
  from {{ ref('dim_users') }}
  join {{ ref('dim_accounts') }} on (dim_users.account_id = dim_accounts.account_id)
  where dim_accounts.is_billable is true
    and dim_users.is_account_owner is true

)

, first_payments as (

  select * from {{ ref('account_first_payment_succeeded') }}

)


, refunds as (

  select * from {{ ref('rails_prod_payment_refunded_stage') }}

)

, refund_count as (

  select
    refunds.account_id
    , refunds.received_at
    , row_number() over (
      partition by refunds.account_id 
      order by refunds.received_at
    ) as refund_number
  from refunds
  join users on (refunds.account_id = users.account_id)

)

, first_refund as (

  select
    account_id
    , received_at
  from refund_count
  where refund_number = 1

)

select
  first_refund.account_id
  , 'first_payment_refunded' as event -- noqa: L029
  , first_payments.subscriber_billing
  , first_refund.received_at
from first_payments
join first_refund on (
  first_payments.account_id = first_refund.account_id
  and date_trunc('month', first_payments.received_at)
  = date_trunc('month', first_refund.received_at) 
  )
where first_refund.received_at >= '2017-11-14'
  and first_payments.received_at > '2017-11-14'
