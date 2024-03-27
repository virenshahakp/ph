{{
  config(
    materialized='incremental'
    , unique_key='playback_session_id'
    , dist='playback_session_id'
    , sort='loaded_at'
    , tags=["exclude_daily"]
  )
}}

{%- set max_loaded_at = incremental_max_value('loaded_at') %}

with

playback_session as (

  select
    *
    , split_part(playback_session_id, '-', 1) as platform
  from {{ ref('dataserver_prod_playback_session_ended_source') }}

)

select * from playback_session
{%- if is_incremental() %}
  where loaded_at > {{ max_loaded_at }}
{%- endif %}
