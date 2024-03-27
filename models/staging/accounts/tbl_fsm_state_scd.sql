{{ 
config(materialized='table', 
sort=['start_time','end_time'], 
dist='user_id' ) 
}}

select
  user_id
  , new_fsm_state                                                                                 as fsm_state
  , received_at                                                                                   as start_time
  , coalesce(lead(received_at, 1) over (partition by user_id order by received_at), '9999-12-30') as end_time
from rails_prod.fsm_state_changed
