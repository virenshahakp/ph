{{ config(materialized='ephemeral') }}
WITH 

channels AS (

  SELECT * FROM {{ ref('guide_channels_source') }}

)

SELECT * FROM channels
