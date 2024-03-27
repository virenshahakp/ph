with

acquisition_funnel as (

  select * from {{ ref('fct_acquisition_funnel') }}

)

select
  acquisition_funnel.account_id
  , 'all-subscribers' as audience
  , 'All Subscribers' as audience_name
from acquisition_funnel
where visited_at is not null
  and subscribed_at is not null
