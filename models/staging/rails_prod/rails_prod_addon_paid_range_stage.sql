with

addon_list as (

  select package from {{ ref('rails_prod_available_addons_stage') }}

)

, cancelled as (

  select * from {{ ref('rails_prod_cancellation_complete_source') }}

)

, users_adding_packages as (

  select distinct account_id
  from {{ ref('rails_prod_package_added_stage') }}

)

, paid_events as (

  select
    account_id    as account_id
    , received_at as received_at
    , package     as package
    , true        as is_active
  from {{ ref('rails_prod_package_payment_succeeded_source') }}
  where package in (select package from addon_list)

)

, dropped_events as (

  select
    account_id    as account_id
    , received_at as received_at
    , package     as package
    , false       as is_active
  from {{ ref('rails_prod_package_dropped_source') }}
  where package in (select package from addon_list)
    and account_id in (select account_id from users_adding_packages)

)

, events as (

  select *
  from paid_events

  union distinct

  select *
  from dropped_events
)

, events_annotated as (

  select
    *
    -- Structure copied from paid_user_subscription_range for computing addon tenures
    , case
      when is_active and lag(is_active) over (partition by account_id, package order by received_at) is null
        then 'start'
      when not is_active and lead(is_active) over (partition by account_id, package order by received_at) is null
        then 'end'
      when not is_active and lag(is_active) over (partition by account_id, package order by received_at)
        then 'end'
      when is_active and not lag(is_active) over (partition by account_id, package order by received_at)
        then 'start'
    end as boundary
  from events

)

, sub_sub_ranges as (

  select
    account_id                                                                       as account_id
    , package                                                                        as package
    , received_at                                                                    as date_range_start
    , boundary                                                                       as boundary
    , lead(received_at) over (partition by account_id, package order by received_at) as dd2
  from events_annotated
  where boundary is not null

)

, sub_ranges as (

  select
    account_id                                                                       as account_id
    , package                                                                        as package
    , date_range_start                                                               as date_range_start
    , coalesce(dd2, getdate())                                                       as date_range_end
    , row_number() over (partition by account_id, package order by date_range_start) as seq
  from sub_sub_ranges
  where boundary = 'start'

)

-- There are no explicit package_dropped events generated when an account is canceled, so fold those
-- events in too, using a cancellation_complete event timestamp (if present), to override date_range_end
, ranges_with_cancellations as (
  select
    sub_ranges.account_id                    as account_id
    , sub_ranges.package                     as package
    , sub_ranges.date_range_start            as date_range_start
    , sub_ranges.date_range_end              as date_range_end
    , getdate() <= sub_ranges.date_range_end as active
    , max(cancelled.received_at)             as cancelled_at
  from sub_ranges
  left join cancelled
    on (
      sub_ranges.account_id = cancelled.account_id
      and cancelled.received_at between sub_ranges.date_range_start and sub_ranges.date_range_end
    )
  group by 1, 2, 3, 4
)

select
  account_id                               as account_id
  , package                                as package
  , active                                 as active
  , date_range_start                       as date_range_start
  , active                                 as is_active
  , coalesce(cancelled_at, date_range_end) as date_range_end
from ranges_with_cancellations
