{{ config(
  materialized='incremental'
  , unique_key='playback_session_id'
  , dist='playback_session_id'
  , sort=['created_at']
  , tags=["daily", "exclude_hourly"]
  )
}}

{%- set max_loaded_at = incremental_max_value('loaded_at') %}

with

playback_session as (

  /*
   there are rare occassions where we have more than one record for a playback session id
   if multiple records arrive in a single incremental batch we will use the last value as
   the authoritative record

   if multiple records arrive across batches then the unique_key will replace the existing
   record with the new record
  */
  select distinct
    playback_session_id
    , last_value(user_id ignore nulls) over (
      partition by playback_session_id
      order by session_created_at asc
      rows between unbounded preceding and unbounded following
    ) as user_id
    , last_value(geohash ignore nulls) over (
      partition by playback_session_id
      order by session_created_at asc
      rows between unbounded preceding and unbounded following
    )  as geohash
    , last_value(dma ignore nulls) over (
      partition by playback_session_id
      order by session_created_at asc
      rows between unbounded preceding and unbounded following
    )  as dma_code
    , last_value(session_created_at ignore nulls) over (
      partition by playback_session_id
      order by session_created_at asc
      rows between unbounded preceding and unbounded following
    )  as session_created_at
    , last_value(loaded_at ignore nulls) over (
      partition by playback_session_id
      order by loaded_at asc
      rows between unbounded preceding and unbounded following
    )  as loaded_at
  from {{ ref('dataserver_prod_playback_session_created_stage') }}
  {% if is_incremental() %}
    where loaded_at > {{ max_loaded_at }}
  {%- endif %}

)

, dma_lookup as (

  select * from {{ ref('dma_code_name') }}

)

select
  playback_session.playback_session_id
  , playback_session.user_id
  , playback_session.dma_code
  , playback_session.geohash
  , dma_lookup.name                     as dma_name
  , playback_session.session_created_at as created_at
  , playback_session.loaded_at
from playback_session
left join dma_lookup on (playback_session.dma_code = dma_lookup.code)