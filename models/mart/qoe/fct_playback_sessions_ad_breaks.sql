{{
  config(
    materialized='incremental'
    , unique_key='playback_session_id'
    , dist='playback_session_id'
    , sort='loaded_at'
  )
}}

{%- set max_loaded_at = incremental_max_value('loaded_at') %}

with

parsing_ads as (

  select
    playback_session_id
    , user_id
    , is_new_session
    , is_sender
    , shared_playback_session_id
    , session_created_at
    , json_parse(ad_breaks)::super as ad_breaks
    , loaded_at
  from {{ ref('dataserver_prod_playback_session_created_stage') }}
  where
    is_valid_json_array(ad_breaks) is true
    {% if is_incremental() %}
      and loaded_at > {{ max_loaded_at }}
    {% endif %}

)

select
  t_ad_breaks.playback_session_id
  , t_ad_breaks.user_id
  , t_ad_breaks.is_new_session
  , t_ad_breaks.is_sender
  , t_ad_breaks.shared_playback_session_id
  , t_ad_breaks.session_created_at
  , breaks.start as start_ms
  , breaks.end   as end_ms
  , index        as ad_break_index --noqa: RF02 
  , t_ad_breaks.loaded_at
from parsing_ads as t_ad_breaks, t_ad_breaks.ad_breaks as breaks at index --noqa: RF02 t 