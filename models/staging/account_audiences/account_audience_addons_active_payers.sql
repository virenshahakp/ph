with

addon_ranges as (

  select * from {{ ref('fct_addon_subscription_range') }}

)

select distinct
  account_id               as account_id
  , 'addons-current-payer' as audience
  , 'Addons Active Payer'  as audience_name
from addon_ranges
where paid_active is true