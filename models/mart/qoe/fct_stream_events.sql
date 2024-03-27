{{
  config(
    materialized='incremental'
    , unique_key='event_id'
    , dist='playback_session_id'
    , sort=['user_id', 'received_at', 'event_timestamp']
  )
}}

{%- set max_dbt_processed_at = incremental_max_value('dbt_processed_at') %}

with

events as (

  select
    -- exclude=["dbt_processed_at"]
    id
    , user_id
    , hashed_session_id
    , event_timestamp
    , received_at
    , app_version
    , analytics_version
    , os_version
    , client_ip
    , device_name
    , device_manufacturer
    , device_model
    , event
    , playback_session_id
    , asset_id
    , adapted_bitrate
    , user_selected_bitrate
    , estimated_bandwidth
    , is_wifi
    , is_cellular
    , position
    , screen_height
    , screen_width
    , loaded_at
    , error_code
    , error_description
    , error_philo_code
    , error_detailed_name
    , error_http_status_code
    , is_buffering
    , is_errored
    , platform
    , event_id
    , played_asset_id
    , requested_asset_id
    , duration
  from {{ ref('all_platforms_stream_events') }}
  {%- if is_incremental() %}
    where dbt_processed_at > {{ max_dbt_processed_at }}
  {%- endif %}

)

, events_to_recalculate as (

  select *
  from events
  
  {%- if is_incremental() %}
    union distinct -- not union all, we want distinct elements 

    select
      -- exclude=["dbt_processed_at", "event_index", "count_events"] to match the query order above
      id
      , user_id
      , hashed_session_id
      , event_timestamp
      , received_at
      , app_version
      , analytics_version
      , os_version
      , client_ip
      , device_name
      , device_manufacturer
      , device_model
      , event
      , playback_session_id
      , asset_id
      , adapted_bitrate
      , user_selected_bitrate
      , estimated_bandwidth
      , is_wifi
      , is_cellular
      , position
      , screen_height
      , screen_width
      , loaded_at
      , error_code
      , error_description
      , error_philo_code
      , error_detailed_name
      , error_http_status_code
      , is_buffering
      , is_errored
      , platform
      , event_id
      , played_asset_id
      , requested_asset_id
      , duration
    from {{ this }}
    where playback_session_id in (
        select playback_session_id from events
      )
  {% endif %}

)

-- deduplicate events that are being reprocessed and have updated asset id and user id values
, unique_events as (

  select distinct
    id
    , hashed_session_id
    , event_timestamp
    , received_at
    , app_version
    , analytics_version
    , os_version
    , client_ip
    , device_name
    , device_manufacturer
    , device_model
    , event
    , playback_session_id
    , asset_id
    , adapted_bitrate
    , user_selected_bitrate
    , estimated_bandwidth
    , is_wifi
    , is_cellular
    , position
    , screen_height
    , screen_width
    , loaded_at
    , error_code
    , error_description
    , error_philo_code
    , error_detailed_name
    , error_http_status_code
    , is_buffering
    , is_errored
    , platform
    , event_id
    , requested_asset_id
    , duration
    , first_value(user_id ignore nulls)
      over (partition by event_id order by event_timestamp rows between unbounded preceding and unbounded following)
    as user_id
    , first_value(played_asset_id ignore nulls)
      over (partition by event_id order by event_timestamp rows between unbounded preceding and unbounded following)
    as played_asset_id
  from events_to_recalculate

)


, events_with_index as (

  select
    *
    , row_number() over (
      partition by playback_session_id
      order by event_timestamp asc
    ) as event_index
    , count(1) over (
      partition by playback_session_id
    ) as count_events
  from unique_events

)

select
  *
  , sysdate as dbt_processed_at
from events_with_index
