with

acquisition_funnel as (

  select * from {{ ref('fct_acquisition_funnel') }}

)

select
  acquisition_funnel.account_id
  , 'all-visitors' as audience
  , 'All Visitors' as audience_name
from acquisition_funnel
where visited_at is not null