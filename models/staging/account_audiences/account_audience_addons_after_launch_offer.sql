/* addon launch offer ended on 2020-07-15 this captures all addons trialers & payers after that point */

with

addon_ranges as (

  select * from {{ ref('fct_addon_subscription_range') }}

)

select distinct
  addon_ranges.account_id       as account_id
  , 'addons-post-launch-offer'  as audience
  , 'Addons After Launch Offer' as audience_name
from addon_ranges
where trial_start >= '2020-07-15'
  and (paid_start >= '2020-07-15' or paid_start is null)
  and (proration_start >= '2020-07-15' or proration_start is null)