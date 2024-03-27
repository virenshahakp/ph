with

addon_ranges as (

  select * from {{ ref('fct_addon_subscription_range') }}

)

select distinct
  account_id          as account_id
  , 'addons-trialer'  as audience
  , 'Addons Trialers' as audience_name
from addon_ranges