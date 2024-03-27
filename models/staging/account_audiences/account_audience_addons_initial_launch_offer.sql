/* all addons trials & pays before 2020-07-15 were participants in the launch offer */

with

addon_ranges as (

  select * from {{ ref('fct_addon_subscription_range') }}

)

select distinct
  addon_ranges.account_id as account_id
  , 'addons-launch-offer' as audience
  , 'Addons Launch Offer' as audience_name
from addon_ranges
where trial_start < '2020-07-15'
  or paid_start < '2020-07-15'
  or proration_start < '2020-07-15'