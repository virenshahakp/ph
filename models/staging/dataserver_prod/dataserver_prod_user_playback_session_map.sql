{{ 
  config(materialized = 'incremental'
    , unique_key='playback_session_id' 
    , dist='playback_session_id'
    , sort=['user_id', 'playback_session_id', 'received_at']
    , tags=["hourly", "daily"]
  )
}}

/*************
* We incrementally append to this user & asset => playback session mapping.
* The additional complexity for incremental events and the row number is because
* there are occassions where multiple events are sent from data server for the same playback session.
* We are choosing to trust the first event.
**************/

{%- set max_loaded_at = incremental_max_value('loaded_at') %}

with

playback_sessions as (

  select *
  from {{ ref('dataserver_prod_playback_session_created_stage') }} as playback_sessions --noqa: L031
  {%- if is_incremental() %}
    -- only process new records
    where loaded_at > {{ max_loaded_at }}
      and not exists (
        select 1 as exists_check
        from {{ this }} as existing_sessions --noqa: L031
        where playback_sessions.playback_session_id = existing_sessions.playback_session_id
      )
  {%- endif %}

)


, add_row_number as (

  select
    user_id
    , playback_session_id
    , played_asset_id
    , received_at
    , loaded_at
    , row_number() over (partition by playback_session_id order by received_at) as rn
  from playback_sessions
  where
    user_id is not null
    and played_asset_id is not null
  qualify rn = 1

)

select
  user_id
  , playback_session_id
  , played_asset_id
  , received_at
  , loaded_at
from add_row_number
{%- if target.name != 'prod' %}
  where received_at >= {{ dbt.dateadd('day', -incremental_dev_mode_days(), 'current_date') }}
{%- endif -%}