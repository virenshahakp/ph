{{ config(materialized='view') }}
WITH

historic_and_recent AS (

  {{ dbt_utils.union_relations(
      relations=[
          ref('playback_sessions_historic')
        , ref('playback_sessions_recent')
      ]
    )
  }}

)

SELECT * FROM historic_and_recent
-- Make sure we have a playback_session_created event
WHERE sutured_pid IS NOT NULL
  -- Ignore targeting sessions for now
  AND is_sender IS FALSE
  -- Make sure we have at least a start. Many platforms
  -- do not send an end if the user exits the app mid-stream,
  -- so we do not guarantee an end time in this table.
  AND stream_start_count >= 1
