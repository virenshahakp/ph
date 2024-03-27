with

acquisition_funnel as (

  select * from {{ ref('fct_acquisition_funnel') }}

)

, trial_starts as (

  select * from {{ ref('rails_prod_trial_started_stage') }}

)

, cancellation_complete as (

  select * from {{ ref('rails_prod_cancellation_complete_stage') }}

)

select
  acquisition_funnel.account_id
  , 'subscribed-never-paid'  as audience
  , 'Subscribed, Never Paid' as audience_name
from acquisition_funnel
left join trial_starts on (acquisition_funnel.account_id = trial_starts.account_id)
left join cancellation_complete on (acquisition_funnel.account_id = cancellation_complete.account_id)
where acquisition_funnel.visited_at is not null
  and trial_starts.received_at is not null
  and acquisition_funnel.subscribed_at is not null
  and acquisition_funnel.first_paid_at is null
  and cancellation_complete.received_at is not null