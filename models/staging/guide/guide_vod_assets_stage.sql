{{ config(materialized='ephemeral') }}
WITH 

vod_assets AS (

  SELECT * FROM {{ ref('guide_vod_assets_source') }}

)

-- There are duplicates in the source data which we need to clean up. For now,
-- de-duplicate here.
SELECT DISTINCT * FROM vod_assets
