with

all_accounts as (

  select * from {{ ref('dim_accounts') }}

)

select
  account_id
  , 'allBillableAccounts'   as audience
  , 'All Billable Accounts' as audience_name
from all_accounts
where all_accounts.is_billable is true