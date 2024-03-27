{{ config(materialized='ephemeral') }}
WITH 

recordings AS (

  SELECT * FROM {{ ref('guide_recordings_source') }}

)

-- There are duplicates in the source data which we need to clean up. For now,
-- de-duplicate here.
SELECT DISTINCT * FROM recordings
