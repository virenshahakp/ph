{{
  config(
    materialized='table'
    , dist='account_id'
    , sort=['account_id', 'date_range_start_at', 'date_range_end_at']
    )
}}
with

-- rails sync events indicating access to a subscription
access_records as (

  select
    reported_at
    , subscriber_billing
    , account_id
    , product_sku
    , activated_plan
    , status
    , subscription_hash
  from {{ ref('rails_sync_users_with_content_access_stage') }}
  where
    subscriber_billing not in ('edu', 'unbilled')

)

/*
 the distinct set of dates reports have been generated
 this is used to detect gaps in records where an account
 was lapsed for a period of time and then restarted with
 the same subscription and biller
*/
, reporting_timestamps as (

  select distinct reported_at
  from access_records

)

-- for finding gaps we need to know when the adjacent reports were delivered
, adjacent_reports as (

  select
    reported_at
    , lead(reported_at) over (order by reported_at) as next_report_sync_at
    , lag(reported_at) over (order by reported_at)  as previous_report_sync_at
  from reporting_timestamps

)

-- identify lead/lag events to be able to detect changes in subscription status
, adjacent_events as (

  select
    access_records.account_id
    , access_records.subscriber_billing
    , access_records.product_sku
    , access_records.activated_plan
    , access_records.status
    , access_records.subscription_hash
    , access_records.reported_at

    -- when would we have expected a previous or next record
    , adjacent_reports.previous_report_sync_at
    , adjacent_reports.next_report_sync_at

    -- what are the previous and next subscriptions
    , lag(access_records.subscription_hash, 1)
      over (partition by access_records.account_id order by access_records.reported_at)
    as lag_subscription_hash
    , lead(access_records.subscription_hash, 1)
      over (partition by access_records.account_id order by access_records.reported_at)
    as lead_subscription_hash

    -- what are the previous and next events for this account
    , lag(access_records.reported_at, 1)
      over (partition by access_records.account_id order by access_records.reported_at)
    as lag_reported_at
    , lead(access_records.reported_at, 1)
      over (partition by access_records.account_id order by access_records.reported_at)
    as lead_reported_at
  from access_records
  left join adjacent_reports on (access_records.reported_at = adjacent_reports.reported_at)

)

-- get starting and ending boundaries for consecutive days of access to a specific product/plan
, range_boundaries as (

  select
    account_id
    , subscriber_billing
    , product_sku
    , activated_plan
    , status
    , subscription_hash
    , reported_at

    , lag_reported_at
    , lead_reported_at

    , lag_subscription_hash
    , lead_subscription_hash

    , previous_report_sync_at
    , next_report_sync_at

    , case
      when lag_reported_at is null then reported_at -- first ever event
      when
        subscription_hash != lag_subscription_hash -- something about the subscription changed
        then reported_at
      when
        previous_report_sync_at != lag_reported_at -- there was a gap in reporting
        then reported_at
    end                                                       as date_range_start_at
    , case
      when lead_reported_at is null then next_report_sync_at -- last ever event
      when
        subscription_hash != lead_subscription_hash -- something about the subscription changed
        then next_report_sync_at
      when next_report_sync_at != lead_reported_at -- there is a gap in reporting
        then next_report_sync_at
    end                                                       as date_range_end_at
    , coalesce(lead_reported_at, next_report_sync_at) is null as is_active
  from adjacent_events
  where
    lead_reported_at != next_report_sync_at -- next event skips some amount of time
    or lag_reported_at != previous_report_sync_at -- previous event was after a gap in time
    or lag_reported_at is null  -- no previous event
    or lead_reported_at is null -- no next event
    or subscription_hash != lag_subscription_hash -- subscription changed from previous
    or subscription_hash != lead_subscription_hash -- subscription change at next event

)

-- fill in the end dates via a lead function for each starting record
, tenures as (

  select
    account_id
    , subscriber_billing
    , product_sku
    , activated_plan
    , status
    , subscription_hash
    , reported_at
    , date_range_start_at
    , coalesce(
      date_range_end_at, lead(date_range_end_at) over (partition by account_id order by reported_at)
    ) as date_range_end_at
  from range_boundaries

)

-- use the starting event to report on the tenure
select
  account_id
  , subscriber_billing
  , product_sku
  , activated_plan
  , status
  , subscription_hash
  , date_range_start_at
  , date_range_end_at
  /*
  override the previously defined is active
  now that we are only using the start range
  to determine the details of each tenure
  */
  , date_range_end_at is null as is_active
  /*
  prior to August 13, 2023 FAST users were getting included
  in the sync with a null activated_plan
  we wish to exclude them from the paid ranges and all paid
  users were being reported as 'philo-2021'

  after August 12, 2023 FAST users are excluded from this sync

  after Sept 26, 2023 the specific plans started to be reported
  and null values are allowed.
  */
  , case
    -- only post-trial status can be paid
    when status = 'post-trial'
      then
        case
          -- all post-trial records after 2023-08-13 are paid
          when reported_at > '2023-08-13'
            then true
          -- only 'philo-2021' plans prior to 2023-08-13 are paid
          when activated_plan = 'philo-2021'
            then true
          else false
        end
    else false
  end                         as is_paid_range
from tenures
-- select only the starting boundaries
where reported_at = date_range_start_at