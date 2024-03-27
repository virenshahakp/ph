{% macro playback_session_generate_sql(is_historic) %}

{%- set lookback_days = 14 %}


{%- set max_created_at = incremental_max_value('created_at') %}

{%- set max_ended_at = incremental_max_value('ended_at') %}

WITH
playback_session_created AS (

  SELECT * FROM {{ ref('dataserver_prod_playback_session_created_stage') }}

)

, stream_events AS (

  SELECT * FROM {{ ref('fct_stream_events') }}

)

, session_attributes AS (

  SELECT * FROM {{ ref('playback_session_attributes') }}

)

, session_error_attributes AS (

  SELECT * FROM {{ ref('playback_session_error_attributes') }}

)

, events_incremental AS (

  SELECT *
  FROM stream_events
  WHERE
  {%- if is_historic %}
    received_at < {{ dbt.dateadd('day', -incremental_recent_days(), 'current_date') }}
    {%- if is_incremental() %}
      AND received_at > {{ max_ended_at }}
    {%- endif %}
  {%- else %}
    received_at >= {{ dbt.dateadd('day', -incremental_recent_days(), 'current_date') }}
  {%- endif %}

)

, events_incremental_agg AS (

  SELECT playback_session_id
    -- Count of events
    , SUM(
      CASE WHEN event = 'rebuffering_start'
        THEN 1
        ELSE 0
      END
      ) AS rebuffering_start_count
    , SUM(
      CASE WHEN event = 'rebuffering_end'
        THEN 1
        ELSE 0
      END
      ) AS rebuffering_end_count
    , SUM(
      CASE WHEN event = 'stream_error'
        THEN 1
        ELSE 0
      END
      ) AS stream_error_count
    , SUM(
      CASE WHEN event = 'stream_start'
        THEN 1
        ELSE 0
      END
      ) AS stream_start_count
    , SUM(
      CASE WHEN event = 'stream_end'
        THEN 1
        ELSE 0
      END
      ) AS stream_end_count

    -- Meaningful durations
    , SUM(
      CASE WHEN event = 'rebuffering_end'
        THEN duration
        ELSE 0
      END
      ) AS rebuffering_duration_total
    -- startup_duration of null indicates we didn't get stream_start
    , MAX(
      CASE WHEN event = 'stream_start'
        THEN duration
        ELSE NULL
      END
      ) AS startup_duration_max

    -- Abandonment counts. These shouldn't be greater than 1
    -- unless clients misbehave and send multiple stream_end events
    , SUM(
      CASE WHEN event = 'stream_end' and is_buffering
        THEN 1
        ELSE 0
      END
      ) AS abandonment_buffering_count
    , SUM(
      CASE WHEN event = 'stream_end' and is_errored
        THEN 1
        ELSE 0
      END
      ) AS abandonment_error_count

    -- Start and end position
    , MAX(
      CASE WHEN event_index = 1
        THEN position
        ELSE -1
      END
      ) AS position_start
    , MAX(
      CASE WHEN event_index = count_events
        THEN position
        ELSE -1
      END
      ) AS position_stop

    -- Start and end
    , MIN(received_at) AS started_at
    , MAX(received_at) AS ended_at
  FROM events_incremental
  GROUP BY playback_session_id

)

{%- if is_incremental() %}

, session_created_incremental AS (

  SELECT playback_session_id
  FROM playback_session_created
  WHERE
  {%- if is_historic %}
    received_at < {{ dbt.dateadd('day', -incremental_recent_days(), 'current_date') }}
    {%- if is_incremental() %}
      AND received_at > {{ max_created_at }}
    {%- endif %}
  {%- else %}
    received_at >= {{ dbt.dateadd('day', -incremental_recent_days(), 'current_date') }}
  {%- endif %}

)

, events_to_update_agg AS (

  SELECT playback_session_id
    , rebuffering_start_count
    , rebuffering_end_count
    , stream_error_count
    , stream_start_count
    , stream_end_count
    , rebuffering_duration_total
    , startup_duration_max
    , abandonment_buffering_count
    , abandonment_error_count
    , position_start
    , position_stop
    , started_at
    , ended_at
  FROM {{ this }} AS this
  WHERE ended_at > {{ dbt.dateadd('day', -lookback_days, max_ended_at) }}
    -- Any playback session that was created since our max created_at
    -- and has had events added to it since our last update
  AND (
    EXISTS (
      SELECT 1
      FROM events_incremental_agg
      WHERE events_incremental_agg.playback_session_id = this.playback_session_id
    ) OR
    EXISTS (
      SELECT 1
      FROM session_created_incremental
      WHERE session_created_incremental.playback_session_id = this.playback_session_id
    )
  )
)

, events_agg_unioned AS (

  SELECT * FROM events_incremental_agg
  UNION ALL
  SELECT * FROM events_to_update_agg

)

, stream_events_agg AS (

  SELECT
    playback_session_id
    , SUM(rebuffering_start_count)        AS rebuffering_start_count
    , SUM(rebuffering_end_count)          AS rebuffering_end_count
    , SUM(stream_error_count)             AS stream_error_count
    , SUM(stream_start_count)             AS stream_start_count
    , SUM(stream_end_count)               AS stream_end_count
    , SUM(rebuffering_duration_total)     AS rebuffering_duration_total
    , MAX(startup_duration_max)           AS startup_duration_max
    , SUM(abandonment_buffering_count)    AS abandonment_buffering_count
    , SUM(abandonment_error_count)        AS abandonment_error_count
    , MIN(position_start)                 AS position_start
    , MAX(position_stop)                  AS position_stop
    , MIN(started_at)                     AS started_at
    , MAX(ended_at)                       AS ended_at
  FROM events_agg_unioned
  GROUP BY playback_session_id
)

{%- else %}

, stream_events_agg AS  (

  SELECT * FROM events_incremental_agg

)

{%- endif %}

/*
  We do not include these attributes in the group by above
  because we cannot necessarily trust that these values do
  not change throughout the session, and we do not want to end
  up with duplicate records for a playback session. We also
  can't be confident that stream_start will be there, or that it
  will be the first event of the session, because clients do unpredictible
  things, thus we take the first event of the session, and use its
  attributes.
*/

SELECT
    COALESCE(session_attributes.playback_session_id, stream_events_agg.playback_session_id) AS playback_session_id
    , session_attributes.user_id
    , session_attributes.hashed_session_id
    , session_attributes.asset_id
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
    , stream_events_agg.rebuffering_start_count
    , stream_events_agg.rebuffering_end_count
    , stream_events_agg.stream_error_count
    , stream_events_agg.stream_start_count
    , stream_events_agg.stream_end_count
    , stream_events_agg.rebuffering_duration_total
    , stream_events_agg.startup_duration_max
    , stream_events_agg.abandonment_buffering_count
    , stream_events_agg.abandonment_error_count
    , stream_events_agg.position_start
    , stream_events_agg.position_stop
    , stream_events_agg.started_at
    , stream_events_agg.ended_at
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
    , playback_session_created.received_at AS created_at
    
FROM stream_events_agg
LEFT JOIN session_attributes
  ON stream_events_agg.playback_session_id = session_attributes.playback_session_id
LEFT JOIN session_error_attributes
  ON stream_events_agg.playback_session_id = session_error_attributes.playback_session_id
LEFT JOIN playback_session_created
  ON stream_events_agg.playback_session_id = playback_session_created.playback_session_id


{% endmacro %}
