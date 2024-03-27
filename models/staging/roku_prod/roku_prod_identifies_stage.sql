with

source as (

  select * from {{ ref('roku_prod_identifies_source') }}

)

/*
  There are millions of roku identify messages that had a null
  anonymous_id, so we are backfilling those events to map to the
  first anonymous_id we do have for either the device or the user
  account. In this way we retain 99% of the roku identify events
  that had a null anonymous_id
*/
, generate_backfill as (

  select
    anonymous_id
    , user_id
    , received_at
    , "timestamp"
    , context_device_advertising_id
    , loaded_at
    , first_value(anonymous_id ignore nulls) over (
      partition by context_device_advertising_id
      order by received_at
      rows between unbounded preceding and unbounded following
    ) as backfill_device_anonymous_id
    -- EM: Drake to add device_id to Roku, afterwards will switch to:
    -- , coalesce('context_device_id', 'context_device_advertising_id') as context_device_id
    , first_value(anonymous_id ignore nulls) over (
      partition by user_id
      order by received_at
      rows between unbounded preceding and unbounded following
    ) as backfill_user_anonymous_id
  from source

)

, apply_backfill as (

  select
    user_id
    , received_at
    , "timestamp"
    , context_device_advertising_id
    , loaded_at
    -- EM: same as comment above, will switch after release:
    -- , context_device_id
    , coalesce(
      anonymous_id
      , backfill_user_anonymous_id
      , backfill_device_anonymous_id
    ) as anonymous_id
  from generate_backfill

)

select *
from apply_backfill
where user_id is not null
