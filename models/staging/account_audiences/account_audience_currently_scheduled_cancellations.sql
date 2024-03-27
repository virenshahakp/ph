with

accounts as (

  select * from {{ ref('dim_accounts') }}

)

, paid_user_ranges as (

  select * from {{ ref('fct_paid_user_subscription_range') }}

)

, cancellation_complete as (

  select * from {{ ref('rails_prod_cancellation_complete_stage') }}

)

, cancellation_scheduled as (

  select * from {{ ref('rails_prod_cancellation_scheduled_stage') }}

)

select distinct
  accounts.account_id
  , 'current-scheduled-cancel'         as audience
  , 'Currently Scheduled Cancellation' as audience_name
from accounts
left join cancellation_complete
  on (
    accounts.account_id = cancellation_complete.account_id
  )
join cancellation_scheduled
  on (
    accounts.account_id = cancellation_scheduled.account_id
  )
join paid_user_ranges
  on (
    accounts.account_id = paid_user_ranges.account_id
  )
where cancellation_complete.received_at is null
  and paid_user_ranges.is_active is true -- is current
  and cancellation_scheduled.received_at > paid_user_ranges.date_range_start_at -- is a cancellation for the current paid range


