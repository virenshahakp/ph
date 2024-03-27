{{ config(materialized='ephemeral') }}
WITH 

genres AS (

  SELECT * FROM {{ ref('guide_genres_source') }}

)

-- There are duplicates in the source data which we need to clean up. For now,
-- de-duplicate here.
SELECT DISTINCT * FROM genres