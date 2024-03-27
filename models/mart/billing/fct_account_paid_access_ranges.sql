{{
  config(
    materialized='table'
    , dist='account_id'
    , sort=['date_range_start_at', 'date_range_end_at', 'account_id']
  )
}}

with

user_paid_subscription_ranges as (

  select
    account_id
    , date_range_start_at
    , date_range_end_at
    , subscriber_billing
    , packages
    , is_active
    , lag(date_range_end_at) over (partition by account_id order by date_range_start_at) as previous_range_end_at
  from {{ ref('fct_paid_user_subscription_range') }}

)

, boundaries as (

  select
    account_id
    , date_range_start_at
    , date_range_end_at
    , subscriber_billing
    , packages
    , is_active
    , previous_range_end_at
    , case
      -- no meaningful gap in paid access, don't consider a new range.
      when datediff(hours, previous_range_end_at::timestamp, date_range_start_at::timestamp) between 0 and 1
        then 0 -- not a new range
      else 1 -- a new range
    end                                                                                                            as is_new_range
    , sum(case
      -- no meaningful gap in paid access, don't consider a new range.
      when datediff(hours, previous_range_end_at::timestamp, date_range_start_at::timestamp) between 0 and 1
        then 0 -- not a new range
      else 1 -- a new range
    end)
      over (partition by account_id order by date_range_start_at rows between unbounded preceding and current row)
    as paid_range_number
  from user_paid_subscription_ranges

)

, continuous_ranges as (

  select
    account_id
    , date_range_start_at
    , date_range_end_at
    , subscriber_billing
    , packages
    , previous_range_end_at
    , paid_range_number
    , first_value(subscriber_billing) over (
      partition by account_id, paid_range_number
      order by date_range_start_at
      rows between unbounded preceding and unbounded following
    ) as subscriber_billing_start
    , first_value(packages) over (
      partition by account_id, paid_range_number
      order by date_range_start_at
      rows between unbounded preceding and unbounded following
    ) as packages_start
    , last_value(subscriber_billing) over (
      partition by account_id, paid_range_number
      order by date_range_start_at
      rows between unbounded preceding and unbounded following
    ) as subscriber_billing_end
    , last_value(packages) over (
      partition by account_id, paid_range_number
      order by date_range_start_at
      rows between unbounded preceding and unbounded following
    ) as packages_end
    , last_value(date_range_end_at) over (
      partition by account_id, paid_range_number
      order by date_range_start_at
      rows between unbounded preceding and unbounded following
    ) as paid_access_end_at
    , last_value(is_active) over (
      partition by account_id, paid_range_number
      order by date_range_start_at
      rows between unbounded preceding and unbounded following
    ) as is_active
    , row_number() over (
      partition by account_id, paid_range_number
      order by date_range_start_at
    ) as rn
  from boundaries
  where 1 = 1
  qualify rn = 1

)

select
  account_id
  , date_range_start_at::timestamp as date_range_start_at
  , paid_access_end_at::timestamp  as date_range_end_at
  , subscriber_billing_start
  , subscriber_billing_end
  , packages_start
  , packages_end
  , paid_range_number
  , is_active
from continuous_ranges