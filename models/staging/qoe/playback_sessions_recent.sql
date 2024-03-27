{{
  config(
    materialized='table'
    , sort='ended_at'
    , dist='playback_session_id'
   )
}}

WITH

playback_sessions AS (

  {{ playback_session_generate_sql(is_historic=false)}}

)

SELECT * FROM playback_sessions
