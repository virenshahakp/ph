with

initial_unsaves as (

  -- data loaded from outside of segment
  select * from {{ ref('periscope_views_initial_unsaves_source') }}

)

, unsaves_stored_in_dev as (

  -- data that was put into the wrong schema in segment
  select * from {{ ref('dataserver_dev_show_unsave_source') }}

)

, show_unsaves as (

  -- correctly instrumented show saves
  select * from {{ ref('dataserver_prod_show_unsave_source') }}

)

, all_unsaves as (

  select
    user_id
    , show_id
    , received_at
    , sent_at
    , "timestamp"
  from initial_unsaves
  union all
  select
    user_id
    , show_id
    , received_at
    , sent_at
    , "timestamp"
  from unsaves_stored_in_dev
  union all
  select
    user_id
    , show_id
    , received_at
    , sent_at
    , "timestamp"
  from show_unsaves

)

select
  all_unsaves.*
  , 'unsave' as event_type
from all_unsaves