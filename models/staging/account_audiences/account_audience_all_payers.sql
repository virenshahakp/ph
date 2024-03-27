with

paid_subscribers as (

  select * from {{ ref('fct_paid_user_subscription_range') }}

)

select distinct
  paid_subscribers.account_id
  , 'all-payers' as audience
  , 'All Payers' as audience_name
from paid_subscribers