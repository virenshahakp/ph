with

all_users as (

  select * from {{ ref('dim_accounts') }}

)

select
  account_id
  , 'allProfilesIncludingSuper'   as audience
  , 'All Profiles Include Supers' as audience_name
from all_users