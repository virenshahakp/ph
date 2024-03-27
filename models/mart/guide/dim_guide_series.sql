{{ config(materialized='view') }}

WITH

all_guide_data AS (

  SELECT * FROM {{ ref('dim_guide_metadata' ) }}

)

SELECT DISTINCT
  tms_series_id
  , philo_series_id
  , series_title
FROM all_guide_data