with

initial_saves as (

  -- data loaded from outside of segment
  select * from {{ ref('periscope_views_initial_saves_source') }}

)

, saves_stored_in_dev as (

  -- data that was put into the wrong schema in segment
  select * from {{ ref('dataserver_dev_show_save_source') }}

)

, show_saves as (

  -- correctly instrumented show saves
  select * from {{ ref('dataserver_prod_show_save_source') }}

)

, all_saves as (

  select
    user_id
    , show_id
    , received_at
    , sent_at
    , "timestamp"
  from initial_saves
  union all
  select
    user_id
    , show_id
    , received_at
    , sent_at
    , "timestamp"
  from saves_stored_in_dev
  union all
  select
    user_id
    , show_id
    , received_at
    , sent_at
    , "timestamp"
  from show_saves

)

select
  all_saves.*
  , 'save' as event_type
from all_saves