with

rno_accounts_list as (

  select * from {{ ref('periscope_views_rno_account_list_source') }}

)

select
  account_id
  , 'rno-users-2020'                  as audience
  , 'Roku National Offer User (2020)' as audience_name
from rno_accounts_list
