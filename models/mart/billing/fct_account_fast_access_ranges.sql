{{
  config(
    materialized='table'
    , dist='user_id'
    , sort=['access_started_at', 'user_id']
  )
}}

with

leading_values as (

  select
    user_id
    , subscriber_billing
    , state_change_reason
    , is_user_credentialed
    , state_started_at
    , subscriber_state
    , fast_plan_state
    , lead(subscriber_billing, 1) over (partition by user_id order by state_started_at asc)  as next_biller
    , lead(state_change_reason, 1) over (partition by user_id order by state_started_at asc) as next_state_change_reason
    , lead(state_started_at, 1) over (partition by user_id order by state_started_at asc)    as next_state_changed_at
    , lead(subscriber_state, 1) over (partition by user_id order by state_started_at asc)    as next_fsm_state
    , lead(fast_plan_state, 1) over (partition by user_id order by state_started_at asc)     as next_fast_plan_state
  from {{ ref('rails_prod_fast_plan_state_changed_stage') }}

)

, access_ranges as (

  select
    user_id
    , is_user_credentialed
    , fast_plan_state
    , next_biller                                              as next_biller
    , subscriber_state                                         as user_fsm_state
    , state_started_at                                         as access_started_at
    , state_change_reason                                      as access_start_reason
    , coalesce(next_state_changed_at, '9999-12-31')::timestamp as access_ended_at
    , next_state_change_reason                                 as access_end_reason
    , next_fsm_state
    , next_fast_plan_state
    , coalesce(subscriber_billing, 'unbilled')                 as starting_biller
  from leading_values
  where fast_plan_state = 'enrolled'


)

select
  user_id
  , is_user_credentialed
  , fast_plan_state
  , starting_biller
  , next_biller
  , user_fsm_state
  , access_started_at
  , access_start_reason
  , access_ended_at
  , access_end_reason
  , next_fsm_state
  , next_fast_plan_state
from access_ranges