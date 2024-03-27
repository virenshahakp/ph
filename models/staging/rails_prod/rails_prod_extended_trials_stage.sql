{{
    config(
      re_data_monitored=false
    )
}}
-- Some early test trial durations were 7 days + 1 second, which we should treat as 7 days
-- Anything else longer is considered an extended free trial
{% set seven_day_trial_duration = 604801 %}

-- migrated from BI dashboards, todo: adjust for rule L031
-- noqa: disable=L031

select
  trial_started.account_id                as account_id
  , trial_started.received_at             as trial_start
  , trial_started.overall_trial_ends_at   as trial_end
  , trial_started.overall_trial_duration  as duration
  , coupon_applied.coupon_code    as coupon_code
from {{ ref('rails_prod_trial_started_stage') }}           as trial_started
join
  {{ ref('rails_prod_apply_coupon_succeeded_stage') }}  as coupon_applied  on
    trial_started.account_id = coupon_applied.account_id
    -- Allow for an extended trial coupon to be applied after a trial has started
    and abs(datediff(second, trial_started.received_at, coupon_applied.received_at)) < {{ seven_day_trial_duration }}
where trial_started.overall_trial_duration > {{ seven_day_trial_duration }}
order by trial_started.received_at
