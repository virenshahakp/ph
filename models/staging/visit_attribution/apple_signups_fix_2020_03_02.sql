{{ config(materialized='ephemeral') }}

/*
This view is not materialized, because it is only used in signed_up_all_sources which materializes the output we use.


Apple devices had a bug where the anonymous_id was set as the user_id before the anonymous_id is linked to the user_id with
the segment identify call.
This meant we lost signups from apple sources because the anonymous id used for the signup was reset before identify was
called (identify links the anonymous_id to the user_id).
signup_auth_success is the last event in signup that fires before identifying the user_id with an anonymous id.
tvOS has a bug that is preventing the signup_auth_success event from firing, so signup_auth_submit is used.
the best was to link the identify with the previous funnel steps is via the context_device_id.  This is supposed to be
unique per device and allows us to link the devices.  There is a 10 minute window where auth codes are valid, that is why we
use a 10 minute range for the timestamps.
*/

with

ios_identify_events as (

  select * from {{ ref('ios_prod_identifies_stage') }}

)

, ios_signup_auth as (

  select * from {{ ref('ios_prod_signup_auth_success_stage') }}

)

, tvos_identify_events as (

  select * from {{ ref('tvos_prod_identifies_stage') }}

)

, tvos_signup_auth as (

  select * from {{ ref('tvos_prod_signup_auth_submit_stage') }}

)

, ios_identifies as (

  select
    ios_signup_auth.anonymous_id
    , ios_identify_events.user_id
    , 'ios'                                as platform
    , min(ios_identify_events.received_at) as "timestamp"
  from ios_identify_events
  join ios_signup_auth
    on (
      ios_identify_events.context_device_id = ios_signup_auth.context_device_id
      and {{ dbt.datediff(
            'ios_identify_events.timestamp'
            , 'ios_signup_auth.timestamp'
            , 'minute')
        }} between 0 and 10
    )
  where ios_signup_auth.environment_analytics_version <= 10
  -- the apple bug has been fixed in analytics version 11.  The identify event does not contain the analytics version
  {{ dbt_utils.group_by(n=3) }}

)

, tvos_identifies as (

  select
    tvos_signup_auth.anonymous_id
    , tvos_identify_events.user_id
    , 'tvos'                                as platform
    , min(tvos_identify_events.received_at) as "timestamp"
  from tvos_identify_events
  join tvos_signup_auth
    on (
      tvos_identify_events.context_device_id = tvos_signup_auth.context_device_id
      and {{ dbt.datediff(
            'tvos_identify_events.timestamp'
            , 'tvos_signup_auth.timestamp'
            , 'minute')
        }} between 0 and 10
    )
  where tvos_signup_auth.environment_analytics_version <= 10
  -- the apple bug has been fixed in analytics version 11.  The identify event does not contain the analytics version
  {{ dbt_utils.group_by(n=3) }}

)

select
  anonymous_id
  , user_id
  , platform
  , "timestamp"
  , "timestamp" as loaded_at -- for later incremental processing
from ios_identifies

union distinct

select
  anonymous_id
  , user_id
  , platform
  , "timestamp"
  , "timestamp" as loaded_at -- for later incremental processing
from tvos_identifies
