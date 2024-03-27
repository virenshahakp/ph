{{
  config(
     materialized='incremental'
     , unique_key='playback_session_id'
     , sort='ended_at'
     , dist='playback_session_id'
     , on_schema_change = 'append_new_columns'
   )
}}

with

playback_sessions as (

  {{ playback_session_generate_sql(is_historic=true) }}

)

select * from playback_sessions