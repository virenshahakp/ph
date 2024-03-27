{{
  config(
    materialized='incremental'
    , dist='playback_session_id'
    , sort=['playback_session_id', 'dbt_processed_at', 'timestamp']
    , on_schema_change='append_new_columns'
  )
}}

/*
  This model stages the player state changed events so that they are
  distributed and sorted to build watched ranges for this platform.

  We stage it by playback_session_id to be able to handle out of order and
  late arriving data which regularly occurs.
*/ 

{%- set max_loaded_at = incremental_max_value('loaded_at') %}

with

player_state_changed as (

  select * from {{ ref('android_prod_player_state_changed_source') }}

)

select
  event_id -- used for unique_key updates in child models
  , user_id
  , playback_session_id
  , received_at
  , "timestamp"
  , delay
  , position_start
  , position_stop
  , hashed_session_id
  , context_ip
  , action
  , asset_id
  , bitrate
  , sequence_number
  , loaded_at
  , f_base64decode(asset_id) as decoded_asset_id
  , sysdate                  as dbt_processed_at
from player_state_changed
{%- if is_incremental() %}
  where loaded_at > {{ max_loaded_at }}
{%- endif %}