{{
    config(
      re_data_monitored=false
    )
}}

-- This value matches the reference date cited in `.../rails/deployments/081_tv.rb` for addons feature launch
{% set addons_feature_launch_date = '2020-06-01 15:00:00' %}


with

-- complete dunning (multiple payment failures leading to deactivation) will trigger a subscription cancellation
-- so only worry about whether a user has billed-through, which will provide content access during proration
payment_success_events as (

  select
    account_id
    , package
    , received_at
  from {{ ref('rails_prod_package_payment_succeeded_source') }}
  where received_at > '{{ addons_feature_launch_date }}'
    and package in (select package from {{ ref('rails_prod_available_addons_stage') }})

)

, payment_failed_events as (

  select
    package_payment_failed_id as id
    , account_id
    , received_at
    , package
  from {{ ref('rails_prod_package_payment_failed_source') }}
  where received_at > '{{ addons_feature_launch_date }}'
    and package in (select package from {{ ref('rails_prod_available_addons_stage') }})

)

, bill_events as (

  select
    account_id
    , received_at
  from {{ ref('rails_prod_payment_succeeded_source') }}
  where amount > 0

)

, subscription_cancellations as (

  select
    account_id
    , received_at
  from {{ ref('rails_prod_cancellation_complete_source') }}
  where received_at > '{{ addons_feature_launch_date }}'

)

, starts_with_trial as (

  select
    account_id
    , package
    , trial_end as proration_start
  from {{ ref('rails_prod_addon_trial_range_stage') }}
  -- user must have proceeded beyond the trial
  where product_dropped is null
    and subscription_cancelled is null

)

, starts_without_trial as (

  select
    account_id
    , package
    , received_at as proration_start
  from {{ ref('rails_prod_package_added_stage') }}
  -- once a trial has been used, a user can still toggle an addon on/off, but there is no trial time remaining
  where trial_remaining = 0
    and package in (select package from {{ ref('rails_prod_available_addons_stage') }})

)

, all_starts as (

  select * from starts_with_trial

  union all

  select * from starts_without_trial

)

, proration_details as (
  select
    all_starts.account_id
    , all_starts.package
    , all_starts.proration_start as start -- noqa: L029
    , min(all_starts.proration_start) as proration_start
    , max(pd.received_at) as product_dropped
    , min(payment_success_events.received_at) as payment_succeeded
    , min(be1.received_at) as bill_succeeded
    , min(be2.received_at) as first_billed
    -- use max() here since the user will not show up as paid until a payment success,
    -- but we are still on the hook for paying for content access during dunning
    , max(payment_failed_events.received_at) as payment_failed
    , count(distinct payment_failed_events.id) as payment_failures
    , min(subscription_cancellations.received_at) as subscription_cancelled
  from all_starts
  -- left join to find the payment events (success or failure) *after* proration start (if any)
  left join payment_success_events
    on  all_starts.account_id = payment_success_events.account_id
      and all_starts.package = payment_success_events.package
      and payment_success_events.received_at between all_starts.proration_start and (
        all_starts.proration_start + '37 days'::interval
      )
  left join bill_events as be1
    on  all_starts.account_id = be1.account_id
      and be1.received_at between all_starts.proration_start and (all_starts.proration_start + '37 days'::interval)
  left join bill_events as be2
    on  all_starts.account_id = be2.account_id
  left join payment_failed_events
    on  all_starts.account_id = payment_failed_events.account_id
      and all_starts.package = payment_failed_events.package
      and payment_failed_events.received_at between all_starts.proration_start and (
        all_starts.proration_start + '37 days'::interval
      )
  -- left join to find the earliest outright cancellation *after* proration start (if any)
  left join subscription_cancellations
    on  all_starts.account_id = subscription_cancellations.account_id
      and subscription_cancellations.received_at between all_starts.proration_start and (
        all_starts.proration_start + '37 days'::interval
      )
  -- left join to find the latest product drop event *after* the trial (if any)
  -- dropping a trial *after* the trial will trigger proration, whereas dropping *before* will not
  left join {{ ref('rails_prod_package_dropped_source') }} as pd -- noqa: L031
    on  all_starts.account_id = pd.account_id
      and all_starts.package = pd.package
      and all_starts.proration_start < pd.received_at
  group by 1, 2, 3

)

, prorations as (

  select
    account_id
    , package
    , product_dropped
    -- payment success is best, since it indicates billing and the user should show in the paid list
    -- subscription cancellation clearly indicates loss of content access
    -- payment failure indicates the latest attempt
    -- otherwise, assume still prorating, regardless of dunning
    , subscription_cancelled
    , case
      -- the user has never paid, so content access is stopped at trial end, so do not consider as prorating
      when first_billed is null
        then null
      else proration_start
    end as date_range_start
    , case
      -- the user has never paid, so content access is stopped at trial end, so do not consider as prorating
      when first_billed is null
        then null
      when coalesce(payment_succeeded, bill_succeeded, subscription_cancelled, payment_failed) is null
        then case
          when proration_start + '30 days'::interval < getdate()
            then proration_start + '30 days'::interval
          else getdate()
        end
      else coalesce(payment_succeeded, bill_succeeded, subscription_cancelled, payment_failed, getdate())
    end as date_range_end
    , getdate() <= date_range_end as active
  from proration_details

)

select
  -- In cases where a user toggles on/off a package one of more times within a given proration period,
  -- as defined by the proration end time (date_range_end), 
  -- so group by that and takes the earliest start time (date_range_start)
  account_id                   as account_id
  , package                      as package
  , date_range_end               as date_range_end
  , active                       as active
  , min(date_range_start)        as date_range_start
  , max(product_dropped)         as product_dropped
  , max(subscription_cancelled)  as subscription_cancelled
from prorations
where date_range_start < date_range_end
group by 1, 2, 3, 4
