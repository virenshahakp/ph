-- migrated from BI dashboards, todo: adjust the aliasing for rule L027
-- noqa: disable=L027

with 

payments as (

  select * from {{ ref('rails_prod_payment_succeeded_source') }}

)

, first_payment as (

  select 
    account_id as account_id
    , min("timestamp") as first_paid_at
  from
    payments
  group by 1

)

, non_discounted_payments as (

  select
    account_id
    , amount
    , packages
    , received_at
  from payments
  where is_gift is not true
    and promotion is null    
    -- and not a multi-month offer as those do not currently renew as multi-month subscriptions
    and payments.packages in ('["philo-plus-3m"]', '["philo-plus-6m"]')

)

, identify_full_price as (

  select 
    payments.account_id
    , payments.received_at
    , non_discounted_payments.amount
    , non_discounted_payments.received_at as full_price_received_at
    , row_number() over (
      partition by payments.account_id, payments.received_at order by non_discounted_payments.received_at
    ) as rn
  from payments
  left join non_discounted_payments on (
    payments.account_id = non_discounted_payments.account_id
    and payments.received_at < non_discounted_payments.received_at
    )
  where 
    -- non-gift, non-promotion, & multi-month payments are the only ones we need to identify a full price for
    payments.is_gift is true 
    or payments.promotion is not null
    or payments.packages in ('["philo-plus-3m"]', '["philo-plus-6m"]')

)

, get_full_price as (

  select 
    account_id
    , received_at
    , amount
  from identify_full_price
  where rn = 1 -- first non-discounted payment after a discounted payment

)

select 
  payments.*
  , first_paid_at
  , coalesce(get_full_price.amount, payments.amount) as list_price
from payments
left join first_payment on (payments.account_id = first_payment.account_id)
left join get_full_price on (
  payments.account_id = get_full_price.account_id
  and payments.received_at = get_full_price.received_at
  )
