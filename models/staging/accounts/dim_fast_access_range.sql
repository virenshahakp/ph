{{ 
config(materialized='table', 
sort=['fast_start_time','end_time'], 
dist='account_id' ) 
}}

with eligible_fast_users as (
  select
    user_id
    , start_time
    , end_time
    , case
      when fsm_state = 'deactivated' then 'FAST 1.0'
      when fsm_state = 'known' then 'FAST 1.5'
    end as fast_phase
  from {{ ref('tbl_fsm_state_scd') }}
  -- deactivated users given access in Phase 1
  -- known users can access FAST in Phase 1.5 by deeplinking to content
  where fsm_state in ('deactivated', 'known')
)

, fast_users as (
  select
    user_id       as fast_user_id
    , received_at as fast_start_time
  from rails_prod.product_access_changed
  --product_rule 'fast-content' indicates access to the content
  --'fast-features' tells us about DVR access,still being negotiated 
  where product_rule = 'fast-content'
)

, duplicate_fast_events as (
  select
    eligible_fast_users.user_id
    , fast_users.fast_start_time
    , eligible_fast_users.end_time
    , eligible_fast_users.fast_phase
    , row_number() over (
      partition by eligible_fast_users.user_id, eligible_fast_users.end_time order by fast_users.fast_start_time
    ) as dedupe
  --we can't gurantee only a single fast event
  --this assumes fast will always be be enabled for a user only once 
  --(they never turn off fast AND remain in a deactivated state)
  from eligible_fast_users
  inner join fast_users on eligible_fast_users.user_id = fast_users.fast_user_id
  where fast_users.fast_start_time between eligible_fast_users.start_time and eligible_fast_users.end_time
)

select
  user_id as account_id
  , fast_phase
  , fast_start_time
  , end_time
from duplicate_fast_events
where dedupe = 1




