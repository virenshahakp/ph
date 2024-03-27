{{ config(materialized='ephemeral') }}
WITH 

genres_shows AS (

  SELECT * FROM {{ ref('guide_genres_shows_source') }}

)
-- There are duplicates in the source data which we need to clean up. For now,
-- de-duplicate here.
SELECT DISTINCT * FROM genres_shows