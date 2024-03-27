with

payments as (

  select * from {{ ref('fct_account_payments') }}

)

select
  account_id
  , lifetime_payment_number
  , date_trunc('month', received_at)::date as month  -- noqa: L029
  , case
    when subscriber_billing in ('roku', 'bestbuy')
      then amount - (amount * .10)
    when subscriber_billing in ('apple', 'amazon')
      then amount - (amount * .15)
    when subscriber_billing in ('google')
      then amount - (amount * .075)
    else amount
  end                                      as amount
from payments
