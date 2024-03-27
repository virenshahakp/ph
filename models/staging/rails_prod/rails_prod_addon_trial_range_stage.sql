{{
    config(
      re_data_monitored=false
    )
}}
-- This value matches the reference date cited in `.../rails/deployments/081_tv.rb` for addons feature launch
{% set addons_feature_launch_date = '2020-06-01 15:00:00' %}

-- migrated from BI dashboards, todo: adjust for rule L031
-- noqa: disable=L031

with

addon_list as (

  select package
  from {{ ref('rails_prod_available_addons_stage') }}

)

, subscription_cancellations as (

  select
    received_at
    , account_id
  from {{ ref('rails_prod_cancellation_complete_source') }}
  where received_at > '{{ addons_feature_launch_date }}'

)

, starts_with_trial as (

  select
    pa.account_id
    , pa.package
    , pa.received_at as trial_start
    , pa.received_at + pa.trial_duration * '1 second'::interval as trial_end
    , getdate() <= trial_end as is_active
    , max(pd.received_at) as product_dropped
    , max(subscription_cancellations.received_at) as subscription_cancelled
  from {{ ref('rails_prod_package_added_stage') }} as pa
  -- the left join against package_dropped is to find the last event *during* the trial (if any)
  left join {{ ref('rails_prod_package_dropped_source') }} as pd
    on  pa.account_id = pd.account_id 
      and pa.package = pd.package
      and pd.received_at between pa.received_at and (pa.received_at + pa.trial_duration * '1 second'::interval)
  left join subscription_cancellations
    on  pa.account_id = subscription_cancellations.account_id
      and subscription_cancellations.received_at between pa.received_at and (
        pa.received_at + pa.trial_duration * '1 second'::interval
      )
  where pa.trial_remaining > 0
    -- there should only be 1 (the first) activation where the remaining trial matches the trial duration
    -- the trial duration does not change (typically fixed at 7 days, or 604,800 seconds) as the trial progresses,
    -- when a user toggles the addon on and off; instead the the trial_remaining value decreases over time
    -- NOTE: There are a small number of records where the trial remaining value slightly exceeds the duration
    and pa.trial_remaining >= pa.trial_duration
    and pa.package in (select package from addon_list)
  group by 1, 2, 3, 4

)

select *
from starts_with_trial
