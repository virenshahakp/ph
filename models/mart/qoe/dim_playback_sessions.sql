{{
  config(
    materialized='incremental'
    , dist='playback_session_id'
    , unique_key=['playback_session_id']
    , sort=['created_at', 'playback_session_id']
    , on_schema_change='append_new_columns' 
  )
}}

{%- set max_event_dbt_processed_at = incremental_max_value('dbt_processed_at') %}
{%- set max_session_created_loaded_at = incremental_max_value('playback_session_loaded_at') %}

{% set run_automatic = true %}

{% if ( var("start_date") != "" and var("end_date") != "" ) %}
  {% set run_automatic = false %}
{% endif %}

{% set dates = get_update_dates(2) %}
{% set start_date = dates.start_date %}
{% set end_date = dates.end_date %}

with

new_stream_events_ids as (

  select playback_session_id
  from {{ ref('fct_stream_events') }}
  where
    {% if is_incremental() and run_automatic %}
      dbt_processed_at > {{ max_event_dbt_processed_at }}
    {% else %}
      event_timestamp between '{{ start_date }}'::timestamp and '{{ end_date }}'::timestamp
    {% endif %}

)

, new_playback_session_created_ids as (

  select playback_session_id
  from {{ ref('dataserver_prod_playback_session_created_stage') }}
  where
    {% if is_incremental() and run_automatic %}
      loaded_at > {{ max_session_created_loaded_at }} --  dataserver didn't have dbt_processed at so im using loaded_at  
    {% else %}
      session_created_at between '{{ start_date }}'::timestamp and '{{ end_date }}'::timestamp
    {% endif %}

)

, modified_playback_sessions as (

  select playback_session_id
  from new_stream_events_ids

  union all

  select playback_session_id
  from new_playback_session_created_ids

)

, session_attributes as (

  select *
  from {{ ref('playback_session_attributes') }}
  where playback_session_id in (select playback_session_id from modified_playback_sessions)

)

, session_error_attributes as (

  select *
  from {{ ref('playback_session_error_attributes') }}
  where playback_session_id in (select playback_session_id from modified_playback_sessions)

)

, playback_session_created as (

  select *
  from {{ ref('dataserver_prod_playback_session_created_stage') }}
  where playback_session_id in (select playback_session_id from modified_playback_sessions)

)

, stream_events as (

  select *
  from {{ ref('fct_stream_events') }}
  where playback_session_id in (select playback_session_id from modified_playback_sessions)

)

, events_agg as (

  select
    playback_session_id
    -- Count of events
    , sum(
      case when event = 'rebuffering_start'
          then 1
        else 0
      end
    )                       as rebuffering_start_count
    , sum(
      case when event = 'rebuffering_end'
          then 1
        else 0
      end
    )                       as rebuffering_end_count
    , sum(
      case when event = 'stream_error'
          then 1
        else 0
      end
    )                       as stream_error_count
    , sum(
      case when event = 'stream_start'
          then 1
        else 0
      end
    )                       as stream_start_count
    , sum(
      case when event = 'stream_end'
          then 1
        else 0
      end
    )                       as stream_end_count

    -- Meaningful durations
    , sum(
      case when event = 'rebuffering_end'
          then duration
        else 0
      end
    )                       as rebuffering_duration_total
    -- startup_duration of null indicates we didn't get stream_start
    , max(
      case when event = 'stream_start'
          then duration
      end
    )                       as startup_duration_max

    -- Abandonment counts. These shouldn't be greater than 1
    -- unless clients misbehave and send multiple stream_end events
    , sum(
      case when event = 'stream_end' and is_buffering
          then 1
        else 0
      end
    )                       as is_buffering_at_stream_end -- used to be abandonment_buffering_count
    , sum(
      case when event = 'stream_end' and is_errored
          then 1
        else 0
      end
    )                       as is_errored_at_stream_end -- used to be abandonment_error_count

    -- Start and end position
    , max(
      case when event_index = 1
          then position
        else -1
      end
    )                       as position_start
    , max(
      case when event_index = count_events
          then position
        else -1
      end
    )                       as position_stop

    -- Start and end
    , min(received_at)      as started_at
    , max(received_at)      as ended_at
    , max(dbt_processed_at) as dbt_processed_at
  from stream_events
  group by playback_session_id



)

select
  session_attributes.user_id
  , session_attributes.hashed_session_id
  , session_attributes.is_wifi
  , session_attributes.is_cellular
  , session_attributes.app_version
  , session_attributes.os_version
  , session_attributes.screen_height
  , session_attributes.screen_width
  , session_attributes.client_ip
  , session_attributes.device_name
  , session_attributes.device_manufacturer
  , session_attributes.device_model
  , session_attributes.platform
  , events_agg.rebuffering_start_count
  , events_agg.rebuffering_end_count
  , events_agg.stream_error_count
  , events_agg.stream_start_count
  , events_agg.stream_end_count
  , events_agg.rebuffering_duration_total
  , events_agg.startup_duration_max
  , events_agg.is_buffering_at_stream_end
  , events_agg.is_errored_at_stream_end
  , events_agg.position_start
  , events_agg.position_stop
  , events_agg.started_at
  , events_agg.ended_at
  , session_error_attributes.error_code
  , session_error_attributes.error_description
  , session_error_attributes.error_philo_code
  , session_error_attributes.error_detailed_name
  , session_error_attributes.error_http_status_code
  , playback_session_created.sutured_pid
  , playback_session_created.is_new_session
  , playback_session_created.manifest_environment
  , playback_session_created.content_cdn_host
  , playback_session_created.as_number
  , playback_session_created.as_name
  , playback_session_created.geohash
  , playback_session_created.dma
  , playback_session_created.is_sender
  , playback_session_created.played_asset_id
  , playback_session_created.synthetic_channel_id
  , playback_session_created.received_at                                             as created_at
  , playback_session_created.loaded_at                                               as playback_session_loaded_at
  , events_agg.dbt_processed_at
  , coalesce(session_attributes.asset_id, playback_session_created.played_asset_id)  as asset_id
  , coalesce(session_attributes.playback_session_id, events_agg.playback_session_id) as playback_session_id
from events_agg
left join session_attributes
  on events_agg.playback_session_id = session_attributes.playback_session_id
left join session_error_attributes
  on events_agg.playback_session_id = session_error_attributes.playback_session_id
left join playback_session_created
  on events_agg.playback_session_id = playback_session_created.playback_session_id

