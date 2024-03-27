with

payments_succeeded as (

  select * from {{ ref('rails_prod_payment_succeeded_stage') }}

)

, payments as (

  select
    account_id
    , received_at
    , amount
    , packages
    , rev_share_partner
    , subscriber_billing
    , row_number() over (
      partition by account_id 
      order by received_at
    ) as billing_cycle
  from payments_succeeded
  
)

select
  account_id
  , subscriber_billing
  , received_at
  , amount
  , packages
  -- for use in unioning billing events
  , 'first_payment_succeeded' as event -- noqa: L029
from
  payments
where
  billing_cycle = 1
