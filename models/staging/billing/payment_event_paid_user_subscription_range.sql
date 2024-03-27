{{
  config(
    materialized='table',
    dist='account_id',
    sort='date_range_start',
    tags=['exclude_daily', 'exclude_hourly', 'static']
  )
}}

with

/*
  combine payment & cancellation events for rails (confirmed
  biller payments) & bestbuy billing events.

  confirmed biller payments (vs rails_prod_payment_succeeded)
  will also handle payments that are immediately refunded and
  treat those as cancellations as immediate refunds from our
  agents do not produce the same cancellation events.
*/

completed_events as (

  {{ dbt_utils.union_relations(
     relations=[
       ref('confirmed_biller_payments_stage')
       , ref('rails_prod_cancellation_complete_stage')
       , ref('bby_billing_prod')
     ],
     include=[
         "account_id"
       , "received_at"
       , "activated_at"
       , "packages"
       , "subscriber_state"
       , "subscriber_billing"
       , "amount"
       , "list_price"
       , "is_gift"
       , "is_active"
     ]
    ) 
  }}

)

, cancelling as (

  select * from {{ ref('rails_prod_cancellation_scheduled_stage') }}

)

, events_annotated as (

  select
    completed_events.*
    , case when is_active is true
        and lag(is_active) over (partition by account_id order by received_at) is null
        then 'start'
      when not is_active is true
        and lead(is_active) over (partition by account_id order by received_at) is null
        then 'end'
      when not is_active is true
        and lag(is_active) over (partition by account_id order by received_at) is true
        then 'end'
      when is_active is true
        and not lag(is_active) over (partition by account_id order by received_at) is true
        then 'start'
    end as boundary
  from completed_events

)

, start_end as (

  select
    events_annotated.account_id                                             as account_id
    , events_annotated.packages                                             as packages
    , amount                                                                as first_bill_amount
    , list_price                                                            as auto_renew_amount
    , is_gift                                                               as is_first_bill_gift
    , events_annotated.subscriber_billing                                   as subscriber_billing
    , events_annotated.received_at                                          as date_range_start
    , events_annotated.boundary
    , lead(events_annotated.subscriber_state) over (
      partition by events_annotated.account_id
      order by events_annotated.received_at
    )                                                                       as subscriber_state
    , lead(events_annotated.received_at) over (
      partition by events_annotated.account_id
      order by events_annotated.received_at
    )                                                                       as date_range_end
    , coalesce(events_annotated.activated_at, events_annotated.received_at) as payment_occurs_at
  from events_annotated
  where events_annotated.boundary is not null

)

, sub_ranges as (

  select
    account_id
    , packages
    , subscriber_state
    , subscriber_billing
    , first_bill_amount
    , auto_renew_amount
    , is_first_bill_gift
    , date_range_start
    , payment_occurs_at
    , coalesce(date_range_end, current_timestamp) as date_range_end
    , date_range_end is null                      as is_active
  from start_end
  where boundary = 'start'

)

, cancels_scheduled as (

  select
    sub_ranges.account_id
    , sub_ranges.date_range_end
    , max(cancelling."timestamp") as cancel_scheduled_date
  from sub_ranges
  join cancelling on (sub_ranges.account_id = cancelling.account_id)
  where
    sub_ranges.date_range_end > cancelling."timestamp"
    and sub_ranges.date_range_end <  {{ dbt.dateadd('month', 1, 'cancelling.timestamp') }}
  {{ dbt_utils.group_by(n=2) }}

)

select
  sub_ranges.account_id
  , sub_ranges.packages
  , sub_ranges.subscriber_state
  , sub_ranges.subscriber_billing
  , sub_ranges.first_bill_amount
  , sub_ranges.auto_renew_amount
  , sub_ranges.is_first_bill_gift
  , sub_ranges.date_range_start
  , sub_ranges.date_range_end
  , sub_ranges.is_active
  , sub_ranges.payment_occurs_at
  , cancels_scheduled.cancel_scheduled_date                                                     as cancel_scheduled_at
  , row_number() over (partition by sub_ranges.account_id order by sub_ranges.date_range_start) as seq
from sub_ranges
left join cancels_scheduled on (
  sub_ranges.account_id = cancels_scheduled.account_id
  and sub_ranges.date_range_end = cancels_scheduled.date_range_end
)