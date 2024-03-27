{{
  config(
    materialized='table'
    , dist='account_id'
    , sort=['date_range_start_at', 'date_range_end_at', 'account_id']
  )
}}
with

{% set method_changed_at = '2022-01-12 15:23:00' %}

fsm_state_ranges as (

  select * from {{ ref('rails_prod_fsm_state_changed_stage') }}

)

, cancelling as (

  select * from {{ ref('rails_prod_cancellation_scheduled_stage') }}

)

, payment_event_ranges as (

  select
    account_id
    , packages
    , subscriber_state as subscriber_state
    , subscriber_billing
    , date_range_start as date_range_start_at
    , false            as is_active
    , cancel_scheduled_at
    , 'event_range'    as subscription_method
    , least(
      date_range_end, '{{ method_changed_at }}'
    )                  as date_range_end_at
  from {{ ref('payment_event_paid_user_subscription_range') }}
  where date_range_start < '{{ method_changed_at }}'

)

, rails_sync_ranges as (

  select
    rails_sync_account_access_ranges.account_id
    , rails_sync_account_access_ranges.product_sku as packages
    , fsm_state_ranges.subscriber_state
    , rails_sync_account_access_ranges.subscriber_billing
    , rails_sync_account_access_ranges.date_range_start_at
    , rails_sync_account_access_ranges.date_range_end_at
    , rails_sync_account_access_ranges.is_active
    , null                                         as cancel_scheduled_at
    , 'rails_sync'                                 as subscription_method
    , row_number() over (
      partition by rails_sync_account_access_ranges.account_id, rails_sync_account_access_ranges.date_range_start_at
      order by fsm_state_ranges.state_started_at desc
    )                                              as rn
  from {{ ref('rails_sync_account_access_ranges') }}
  left join fsm_state_ranges
    on (
      rails_sync_account_access_ranges.account_id = fsm_state_ranges.user_id
      and rails_sync_account_access_ranges.date_range_start_at > fsm_state_ranges.state_started_at
      and rails_sync_account_access_ranges.subscriber_billing = fsm_state_ranges.subscriber_billing
    )
  where
    rails_sync_account_access_ranges.is_paid_range is true

)

, rails_sync_with_cancels as (

  select
    rails_sync_ranges.account_id
    , rails_sync_ranges.packages
    , rails_sync_ranges.subscriber_state
    , rails_sync_ranges.subscriber_billing
    , rails_sync_ranges.date_range_start_at
    , rails_sync_ranges.date_range_end_at
    , rails_sync_ranges.is_active
    , cancelling."timestamp" as cancel_scheduled_at
    , rails_sync_ranges.subscription_method
    , row_number() over (
      partition by rails_sync_ranges.account_id, rails_sync_ranges.date_range_start_at
      order by cancelling."timestamp" desc
    )                        as rn
  from rails_sync_ranges
  left join cancelling
    on (
      cancelling."timestamp" between rails_sync_ranges.date_range_start_at
      and rails_sync_ranges.date_range_end_at
      and rails_sync_ranges.account_id = cancelling.account_id
    )
  -- only take the last fsm state+rails_sync record when there are multiple
  where rails_sync_ranges.rn = 1

)

, combined_ranges as (

  select
    account_id
    , packages
    , subscriber_state
    , subscriber_billing
    , date_range_start_at
    , date_range_end_at
    , is_active
    , cancel_scheduled_at
    , subscription_method
  from payment_event_ranges
  union all
  select
    account_id
    , packages
    , subscriber_state
    , subscriber_billing
    , date_range_start_at
    , date_range_end_at
    , is_active
    , cancel_scheduled_at
    , subscription_method
  from rails_sync_with_cancels
  -- only take the last scheduled cancellation event in the date range if there are multiple
  where rn = 1

)

select
  combined_ranges.account_id
  , combined_ranges.packages
  , combined_ranges.subscriber_state
  , combined_ranges.subscriber_billing
  , combined_ranges.date_range_start_at
  , combined_ranges.is_active
  , combined_ranges.cancel_scheduled_at
  , combined_ranges.subscription_method
  , coalesce(combined_ranges.date_range_end_at, getdate())                                                   as date_range_end_at
  , row_number() over (partition by combined_ranges.account_id order by combined_ranges.date_range_start_at) as seq
from combined_ranges