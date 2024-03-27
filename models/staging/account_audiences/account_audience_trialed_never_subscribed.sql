with

acquisition_funnel as (

  select * from {{ ref('fct_acquisition_funnel') }}

)

, trial_starts as (

  select * from {{ ref('rails_prod_trial_started_stage') }}

)

select
  acquisition_funnel.account_id
  , 'trialed-never-subscribed'  as audience
  , 'Trialed, Never Subscribed' as audience_name
from acquisition_funnel
left join trial_starts on (acquisition_funnel.account_id = trial_starts.account_id)
where acquisition_funnel.visited_at is not null
  and (getdate() - trial_starts.received_at) > interval '15 days'
  and trial_starts.received_at is null
  and acquisition_funnel.subscribed_at is null